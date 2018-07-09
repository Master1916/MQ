package org.groovy.push

import com.rabbitmq.client.*
import org.groovy.common.Constants
import org.groovy.dao.merchant.MerchantDao
import org.groovy.dao.terminal.MobileDao
import org.groovy.util.ConvertUtil
import org.groovy.dubbo.PosService
import org.groovy.util.PushMessageUtil
import org.jpos.core.Configuration
import org.jpos.core.ConfigurationException
import org.jpos.q2.QBeanSupport
import org.jpos.util.Log
import org.jpos.util.Logger
import org.jpos.util.NameRegistrar
import org.mobile.mpos.enums.ETransactionType
import org.mobile.mpos.model.PushInfo

/**
 * 一码付消息推送
 */
public class PushMessageOnceTranseInfoHelper extends QBeanSupport {
    private ConnectionFactory factory = null;
    private Connection conn = null;
    private Channel channel = null;
    public String uri = null;
    public String exchangeName = null;
    public String exchangeType = null;
    public Boolean durable = true;
    public int deliveryMode = 0;
    public String msgVersion = null;
    public String prefixRoutingKey = null;
    public int socket_connection_timeout = 60;//连接socket的超时时间
    public int heartbeat_interval_time = 0;//心跳间隔时间，如果为0则为服务端设置的默认值580秒
    public int reconnect_sleep_time = 10;//检测到连接断开后，再次重连之前的间隔时间
    public String queueName = null;
    public String routingKey = null;

    private Log log = new Log(NameRegistrar.getIfExists('logger.Q2') as Logger, PushMessageOnceTranseInfoHelper.getSimpleName());

    @Override
    public void initService() {
        try {
            factory = new ConnectionFactory();
//            factory.setConnectionTimeout(socket_connection_timeout * 1000);
//            factory.setRequestedHeartbeat(heartbeat_interval_time);
            factory.setAutomaticRecoveryEnabled(true)
            factory.setTopologyRecoveryEnabled(true)
            factory.setUri(new URI(uri));
        } catch (Exception e) {
            log.error e
            Thread.sleep(reconnect_sleep_time * 1000);
            close()
        }
    }

    private void initChannel() {
        close();
        conn = factory.newConnection();
        channel = conn.createChannel();
        channel.basicQos(100) //限制取mq消息的数量.
        log.info("**********【立码付】MQ conn [" + conn.toString() + "] connected.***************************");
//            channel.queueDeclare(queueName, durable, false, false, null);
        log.info "【立码付】exchangeName:" + exchangeName
        channel.exchangeDeclare(exchangeName, exchangeType, durable);
        channel.queueDeclare(queueName, true, false, false, null);
        log.info "【立码付】queue:" + queueName
        channel.queueBind(queueName, exchangeName, routingKey)
        log.info "【立码付】routingKey:" + routingKey

        log.info("**********【立码付】MQ channel [" + channel.toString() + "] connected.***************************");
    }

    private void close() {
        if (channel?.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {

            }
        }
        if (conn?.isOpen()) {
            try {
                conn.close();
            } catch (Exception e) {

            }
        }
    }

    @Override
    protected void stopService() throws Exception {
        close()
    }

    @Override
    protected void destroyService() throws Exception {
        close()
    }

    public void setConfiguration(Configuration cfg) throws ConfigurationException {
        uri = cfg.get("uri");
        exchangeName = cfg.get("exchangeName");
        exchangeType = cfg.get("exchange_type");
        durable = cfg.getBoolean("durable");
        msgVersion = cfg.get("msg_version");
        prefixRoutingKey = cfg.get("prefix_routing_key");
        deliveryMode = cfg.getInt("delivery_mode");
        socket_connection_timeout = cfg.getInt("socket_connection_timeout");
        heartbeat_interval_time = cfg.getInt("heartbeat_interval_time");
        reconnect_sleep_time = cfg.getInt("reconnect_sleep_time");
        queueName = cfg.get("queueName");
        routingKey = cfg.get("routing_key");
    }

    private void processMsg(String msg) {
        def voiceNotifyMQ = ConvertUtil.strConvertToMap(msg)
        BigDecimal rmb = new BigDecimal(voiceNotifyMQ?.transAmount).divide(new BigDecimal(100), 2, BigDecimal.ROUND_CEILING)
        log.info "【立码付】解析mq：voiceNotifyMQ=" + voiceNotifyMQ
        if (("sale").equals(voiceNotifyMQ?.transType as String)) {
            def res = getMessage(voiceNotifyMQ?.merchantNo)
            log.info "【立码付】getMessage：res=" + res
            if ("00".equals(res?.code as String)) {
                new PushMessageUtil().pushM(new PushInfo(res.platform as String,
                        res.registrationids as List<String>),
                        Constants.ONCETRANS_PUSH_TIME + voiceNotifyMQ?.transDate + Constants.ONCETRANS_PUSH_MESSGAE + rmb + "元", Constants.ONCETRANS_PUSH_MESSGAE + rmb + "元")
            }
        } else {
            log.info "【立码付】非消费交易：transType=" + voiceNotifyMQ?.transType as String
        }
    }

    @Override
    void startService() throws Exception {
        new Thread() {
            @Override
            void run() {
                while (running()) {
                    try {
                        log.info("initChannel");
                        initChannel();
                        break;
                    } catch (IOException ex) {
                        log.error("队列连接获取失败.............", ex);
                        Thread.sleep(5 * 1000L);
                        continue;
                    }
                }
                channel.basicConsume(queueName, false, "zht-${new Date().format("yyyy-MM-dd HH:mm:ss.SSS")}", new DefaultConsumer(channel) {
                    void handleDelivery(String consumerTag,
                                        Envelope envelope,
                                        AMQP.BasicProperties properties,
                                        byte[] body)
                            throws IOException {
                        String transMsg = new String(body);
                        log.info("transMsg=" + transMsg);
                        processMsg(transMsg);
                        channel.basicAck(envelope.deliveryTag, false)
                    }
                });
                log.info("aaaaabbbbb");
            }
        }.start();
    }

    public def getMessage(def merchantNo) {
        log.info "【立码付】极光推送：merchantNo=" + merchantNo
        MerchantDao merchantDao = new MerchantDao()
        MobileDao mobileDao = new MobileDao()
        //查询是否是POS商户
        def posMerchantInfo = PosService.getMobileByMerchantNo(merchantNo)
        def mobileNo;
        if (posMerchantInfo) {
            mobileNo = posMerchantInfo.telphone;
        } else {
            log.info "【立码付】推送消息：商户不存在 "
            return [
                    code: "09"
            ]
        }
        //根据手机号查询绑定推送消息。
        def registInfo = mobileDao.findMobilePushByMobile(mobileNo)
        //def registInfo = mobileDao.findMobilePushByMobile("13071190677")
        if (registInfo) {
            log.info "【立码付】推送消息：手机推送表：" + registInfo
            return [
                    //13071190677
                    code           : "00",
                    registrationids: [registInfo.registrationid],
                    platform       : registInfo.platform,
            ]
        } else {
            log.info "【立码付】推送消息：手机推送表不存在"
            return [
                    code: "09"
            ]
        }
    }

}
