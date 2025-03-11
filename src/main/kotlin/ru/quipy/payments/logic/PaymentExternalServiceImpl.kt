package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
        val retryableHttpCodes = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val requestAverageProcessingTime = properties.averageProcessingTime

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong()-1, Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        ongoingWindow.acquire()

        try {
            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var waitTimeMillis = 1000L

            while (now() < deadline) {
                try {
                    slidingWindowRateLimiter.tickBlocking()

                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        logger.warn("[$accountName] Недостаточно времени для новой попытки оплаты $paymentId")
                        break
                    }

                    client.newCall(request).execute().use { response ->
                        if (response.isSuccessful) {
                            val responseBodyStr = response.body?.string() ?: ""
                            val body = try {
                                mapper.readValue(responseBodyStr, ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                            logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, result: ${body.result}, message: ${body.message}")
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                            return
                        } else if (response.code in retryableHttpCodes) {
                            waitTimeMillis = (waitTimeMillis + 1).coerceAtMost(3000L)
                            val retryAfterHeader = response.headers["Retry-After"]
                            if (!retryAfterHeader.isNullOrEmpty()) {
                                waitTimeMillis = retryAfterHeader.toLong() * 1000
                            }
                        } else {
                            logger.error("[$accountName] Оплата неуспешна для txId: $transactionId, payment: $paymentId, HTTP код: ${response.code}")
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "HTTP ${response.code}")
                            }
                            return
                        }
                    }
                } catch (e: SocketTimeoutException) {
                    logger.error("[$accountName] SocketTimeout для txId: $transactionId, payment: $paymentId", e)
                } catch (e: Exception) {
                    logger.error("[$accountName] Исключение для txId: $transactionId, payment: $paymentId", e)
                }

                if (now() + waitTimeMillis >= deadline) {
                    break
                }

                Thread.sleep(waitTimeMillis)
            }

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Повторные попытки исчерпаны или дедлайн истёк")
            }
        } finally {
            ongoingWindow.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()