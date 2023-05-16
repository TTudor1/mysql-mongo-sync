package org.disertatie.dbsync;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestConfig extends ResourceConfig {

    public RestConfig() {
        register(Endpoints.class);
        property(ClientProperties.CONNECT_TIMEOUT, 0);
        property(ClientProperties.READ_TIMEOUT, 0);
    }

}
