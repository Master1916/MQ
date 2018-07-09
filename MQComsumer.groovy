package op

import com.rabbitmq.client.*
import org.jpos.q2.QBeanSupport
import org.jpos.util.Log
import service.TransService
import util.Commons
import util.Constants
import util.LoggerUtil

/**
 * @author yinheli
 * @Modifior zjj on 2015/10/22
 */
class MQComsumer extends QBeanSupport {
    Log log = LoggerUtil.getLog(this.getClass().simpleName)

    private String uri
    private String exchange
    private String queueName
    private String routingKey
    private String channelCode

    def channelInfo

    @Override
    protected void initService() throws Exception {
        uri = configuration.get('amqp')
        exchange = configuration.get('exchange')
        queueName = configuration.get('queueName')
        routingKey = configuration.get('routingKey')
        channelCode = Constants.channelCode
    }

    @Override
    protected void startService() throws Exception {
        Thread.start("${channelCode}-mq-comsume-warapper") {
            while (!Constants.SYSTEMSTATUS) {
                log.warn("reload task has not complete!!! sleep 5s")
                sleep(5000L)
            }
            log.info("reload task has complete. start mq-comsume")
            try {
                channelInfo = Commons.dao.findChannel(channelCode)
                startComsume()
            } catch (Exception e) {
                log.error("初始化异常退出系统", e)
                System.exit(0)
            }
        }
    }

    @Override
    protected void stopService() throws Exception {
        try {
            cleanup()
        } catch (ignore) {
        }
    }
    ConnectionFactory connectionFactory
    Connection connection
    Channel channel

    private void startComsume() {
        connectionFactory = new ConnectionFactory()
        connectionFactory.setUri(uri)
        connectionFactory.setAutomaticRecoveryEnabled(true)
        connectionFactory.setTopologyRecoveryEnabled(true)
        connection = connectionFactory.newConnection()

        channel = connection.createChannel()
        channel.basicQos(100) //限制取mq消息的数量.
        channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true)
        channel.queueDeclare(queueName, true, false, false, null)
        channel.queueBind(queueName, exchange, routingKey)

        String consumer = Thread.currentThread().getName() + (new Date().format("yyyyMMddHHmmssSSS"))

        channel.basicConsume(queueName, false, consumer, new DefaultConsumer(channel) {
            @Override
            void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String msg = new String(body, "UTF-8")
                log.info "receive message from mq : ${msg}"
                try {
                    TransService.instance.handleReqTraces(msg, channelInfo.id as int)
                } catch (Exception e) {
                    log.error("TransService error", e)
                }
                //send ack to MQ
                try {
                    channel.basicAck(envelope.getDeliveryTag(), false)
                } catch (ignore) {

                }
            }
        })
    }

    private void cleanup() {
        try {
            channel?.close()
            connection?.close()
        } catch (ignore) {
        }
    }

}
