package org.apache.ranger.services.nifi.client;

/**
 * Config property names from the NiFi service definition.
 */
public interface NiFiConfigs {

    String NIFI_URL = "nifi.url";
    String NIFI_AUTHENTICATION_TYPE = "nifi.authentication";

    String NIFI_SSL_KEYSTORE = "nifi.ssl.keystore";
    String NIFI_SSL_KEYSTORE_TYPE = "nifi.ssl.keystoreType";
    String NIFI_SSL_KEYSTORE_PASSWORD = "nifi.ssl.keystorePassword";

    String NIFI_SSL_TRUSTSTORE = "nifi.ssl.truststore";
    String NIFI_SSL_TRUSTSTORE_TYPE = "nifi.ssl.truststoreType";
    String NIFI_SSL_TRUSTSTORE_PASSWORD = "nifi.ssl.truststorePassword";

    String NIFI_LDAP_USERNAME = "nifi.ldap.username";
    String NIFI_LDAP_PASSWORD = "nifi.ldap.password";

    String NIFI_KERBEROS_PRINCIPAL = "nifi.kerberos.principal";
    String NIFI_KERBEROS_KEYTAB = "nifi.kerberos.keytab";

}
