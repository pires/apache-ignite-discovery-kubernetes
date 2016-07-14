package com.github.pires.ignite.kubernetes;

import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.dns.LookupResult;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Apache Ignite usage on Kubernetes requires that each node (Pod in the Kubernetes lingo)
 * is able to access other pods directly. Proxies will not work.
 * <p>
 * In order to find the desired pods IP addresses, one could query the Kubernetes API for
 * endpoints of a Service but that may prove too much of a hurdle to maintain. Since DNS integration
 * is a common best-practice when assembling Kubernetes clusters, we will rely on SRV lookups.
 */
public class KubernetesPodIpFinder extends TcpDiscoveryVmIpFinder {

    private static final int DNS_LOOKUP_TIMEOUT_MILLIS = 1000;

    /**
     * Grid logger.
     */
    @LoggerResource
    private IgniteLogger log;

    @GridToStringInclude
    private String containerPortName;

    @GridToStringInclude
    private String serviceName;

    public KubernetesPodIpFinder() {
        super(true);
    }

    public KubernetesPodIpFinder(boolean shared) {
        super(shared);
    }

    /**
     * Parses provided container port name.
     *
     * @param containerPortName the container port name to use in lookup queries.
     * @throws IgniteSpiException
     */
    @IgniteSpiConfiguration(optional = true)
    public synchronized void setContainerPortName(String containerPortName) throws IgniteSpiException {
        this.containerPortName = containerPortName;
    }

    /**
     * Parses provided service name.
     *
     * @param serviceName the service name to use in lookup queries.
     * @throws IgniteSpiException
     */
    @IgniteSpiConfiguration(optional = true)
    public synchronized void setServiceName(String serviceName) throws IgniteSpiException {
        this.serviceName = serviceName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Collection<InetSocketAddress> getRegisteredAddresses() {
        // provision DNS resolver for usage with SRV records
        final DnsSrvResolver resolver = DnsSrvResolvers.newBuilder()
                .retainingDataOnFailures(true)
                .dnsLookupTimeoutMillis(DNS_LOOKUP_TIMEOUT_MILLIS)
                .build();
        // resolve configured addresses
        final Collection<InetSocketAddress> inets = new CopyOnWriteArrayList<>();
        final String fqdn = "_" + containerPortName + "._tcp." + serviceName;
        log.debug("Looking up SRV records with FQDN [" + fqdn + "].");
        final List<LookupResult> nodes = resolver.resolve(fqdn);
        for (LookupResult node : nodes) {
            inets.add(InetSocketAddress.createUnresolved(node.host(), node.port()));
        }
        log.debug("Found " + nodes.size() + " nodes.");

        return inets;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(KubernetesPodIpFinder.class, this, "super", super.toString());
    }

}
