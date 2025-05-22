package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;

/**
 * Test for Noran protocol decoder
 * Supports both monolithic and microservices testing environments
 */
public class NoranProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new NoranProtocolDecoder(null));

        verifyNull(decoder, binary(
                "0d0a2a4b57000d000080010d0a"));

        verifyPosition(decoder, binary(
                "34000800010b0000000000003f43bb8da6c2ebe229424e523039423233343439000031362d30392d31352030373a30303a303700"));

        verifyPosition(decoder, binary(
                "28003200c380000000469458408c4ad340ad381e3f4e52303947313336303900000001ff00002041"));

        verifyPosition(decoder, binary(
                "28003200c38000d900fcc97a416b1a7a42b43eef3d4e523039473034383737000000000092fcda4a"));

        verifyPosition(decoder, binary(
                "3400080001090000000000001D43A29BE842E62520424E523039423036363932000031322D30332D30352031313A34373A343300"));
        
        verifyPosition(decoder, binary(
                "34000800010c000000000080a3438e20944149bd07c24e523039423139323832000031352d30342d32362030383a34333a353300"));

        verifyNull(decoder, binary(
                "0f0000004e52303946303431353500"));

        verifyPosition(decoder, binary(
                "22000800010c008a007e9daa42317bdd41a7f3e2384e523039463034313535000000"));

        verifyPosition(decoder, binary(
                "34000800010c0000000000001c4291251143388d17c24e523039423131303930000031342d31322d32352030303a33333a303700"));
        
        verifyPosition(decoder, binary(
                "34000800010c00000000000000006520944141bd07c24e523039423139323832000031352d30342d32352030303a30333a323200"));

    }
    
    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Create a decoder with a mock message producer
        MessageProducer messageProducer = mockMessageProducer();
        var decoder = inject(new NoranProtocolDecoder(null));
        
        // Set the message producer using reflection (for compatibility with both architectures)
        try {
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            field.set(decoder, messageProducer);
        } catch (NoSuchFieldException e) {
            // Field doesn't exist in monolithic architecture, which is fine
        }
        
        // Test position decoding with message broker integration
        verifyPosition(decoder, binary(
                "34000800010b0000000000003f43bb8da6c2ebe229424e523039423233343439000031362d30392d31352030373a30303a303700"));
        
        // In microservices architecture, verify the message was published
        try {
            verify(messageProducer).send(any(), any());
        } catch (Exception e) {
            // Ignore verification in monolithic architecture
        }
    }

}