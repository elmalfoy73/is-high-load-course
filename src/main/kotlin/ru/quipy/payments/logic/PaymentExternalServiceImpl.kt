package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestQueue = ConcurrentLinkedQueue<PaymentRequest>()
    private val executor = Executors.newFixedThreadPool(parallelRequests)

    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000))

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Adding payment request for payment $paymentId to queue")
        requestQueue.add(PaymentRequest(paymentId, amount, paymentStartedAt, deadline))
        processQueue()
    }

    private fun processQueue() {
        while (requestQueue.isNotEmpty()) {
            val request = requestQueue.poll() ?: return
            executor.submit { executePayment(request) }
        }
    }

    private fun executePayment(request: PaymentRequest) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Processing payment for ${request.paymentId}, txId: $transactionId")

        paymentESService.update(request.paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - request.paymentStartedAt))
        }

        var attempt = 0
        val maxRetries = 5
        val maxSlowRetries = 2 // Дополнительные ретраи для долгих запросов
        var delay = 1000L

        while (attempt < maxRetries) {
            limiter.tickBlocking()

            val startTime = now() // Засекаем время начала запроса

            val httpRequest = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${request.paymentId}&amount=${request.amount}")
                post(emptyBody)
            }.build()

            try {
                client.newCall(httpRequest).execute().use { response ->
                    val endTime = now() // Засекаем время окончания запроса
                    val duration = endTime - startTime

                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${request.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), request.paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}, duration: ${duration}ms")

                    if (body.result) {
                        paymentESService.update(request.paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }

                    when (response.code) {
                        400, 401, 403, 405 -> {
                            logger.error("[$accountName] Payment failed permanently for txId: $transactionId, code: ${response.code}")
                            return
                        }
                        439 -> {
                            delay *= 2
                        }
                        503, 500 -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, code: ${response.code}")
                            val retryAfter = response.headers["Retry-After"]?.toLongOrNull()
                            if (retryAfter != null) {
                                delay = retryAfter * 1000
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, code: ${response.code}")
                        }
                    }
                }
            } catch (e: Exception) {
                handleFailure(request, transactionId, e)
            } finally {
                processQueue()
            }

            attempt++
            if (attempt < maxRetries) {
                val jitter = (delay * 0.3 * Math.random()).toLong()
                val finalDelay = min(delay + jitter, abs(request.deadline - now()))

                val adjustedDelay = finalDelay + jitter

                logger.info("Retrying in ${adjustedDelay}ms (attempt ${attempt + 1})")
                Thread.sleep(adjustedDelay)
            }
        }

        paymentESService.update(request.paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Max retries reached")
        }
    }

    private fun handleFailure(request: PaymentRequest, transactionId: UUID, e: Exception) {
        when (e) {
            is SocketTimeoutException -> {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${request.paymentId}", e)
                paymentESService.update(request.paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }
            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${request.paymentId}", e)
                paymentESService.update(request.paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

public fun now() = System.currentTimeMillis()