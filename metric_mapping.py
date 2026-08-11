from dataclasses import dataclass, field
from typing import Callable, Dict, List, Tuple, Optional
from aggregation import (
    AggregateResult,
    aggregate_max,
    aggregate_mean,
    aggregate_sum,
    aggregate_min,
)

@dataclass
class DynatraceToOCIMetric:
    dynatrace_metric_key: str
    aggregation_function: Callable[[List[Dict]], List[AggregateResult]]
    dimension_filter: Dict[str, str] = field(default_factory=dict)

class MetricMapping:
    def __init__(
        self,
        metric_key_map: Dict[str, List[DynatraceToOCIMetric]],
        dimension_map: Dict[str, List[str]],
        constant_dimension_map: Dict[str, str] = {}
    ):
        self.metric_key_map = metric_key_map
        self.dimension_map = dimension_map
        self.constant_dimension_map = constant_dimension_map

    # Given the list of OCI dimensions, this function maps the dimension keys to Dynatrace dimensions
    def dimensions(self, oci_dimensions: Dict[str, str]) -> Dict[str, str]:
        dimensions = self.constant_dimension_map.copy()
        for key, value in oci_dimensions.items():
            dynatrace_dimension_keys = self.dimension_map.get(key)
            if dynatrace_dimension_keys:
                for dynatrace_dimension_key in dynatrace_dimension_keys:
                    dimensions[dynatrace_dimension_key] = value
        return dimensions

    # Given the oci metric name and the list of datapoints, this function returns the dynatrace metric name and the aggregated value
    def value_from_oci_metric_name(
        self, oci_metric_name: str, oci_dimensions: Dict[str, str], datapoints: List[Dict]
    ) -> Optional[Tuple[str, List[AggregateResult]]]:
        metric_mappings = self.metric_key_map.get(oci_metric_name)
        if metric_mappings is None:
            return None


        for metric_mapping in metric_mappings:
            result = (metric_mapping.dynatrace_metric_key, metric_mapping.aggregation_function(datapoints))

            if len(metric_mapping.dimension_filter) == 0:
                return result

            is_filtered_out = True
            for key, value in metric_mapping.dimension_filter.items():
                if (oci_value := oci_dimensions.get(key)) and oci_value == value:
                    is_filtered_out = False

            if is_filtered_out:
                result = None
            else:
                break
        
        return result


""" All compute metric names imported by extension. """
COMPUTE_CPU_UTIL = "cloud.oci.compute.cpu.util"
COMPUTE_DISK_READ_BYTES = "cloud.oci.compute.disk.read.bytes"
COMPUTE_DISK_WRITE_BYTES = "cloud.oci.compute.disk.write.bytes"
COMPUTE_DISK_READ_IO = "cloud.oci.compute.disk.read.io"
COMPUTE_DISK_WRITE_IO = "cloud.oci.compute.disk.write.io"
COMPUTE_LOAD = "cloud.oci.compute.load"
COMPUTE_MEM_ALLOCATION_STALLS = "cloud.oci.compute.memory.allocation.stalls"
COMPUTE_MEM_UTIL = "cloud.oci.compute.memory.util"
COMPUTE_NETWORK_RECEIVE = "cloud.oci.compute.network.receive.bytes"
COMPUTE_NETWORK_TRANSMIT = "cloud.oci.compute.network.transmit.bytes"

""" All load balancer metric names imported by extension. """
LB_HTTP_REQUESTS = "cloud.oci.load_balancer.http.requests"
LB_ACTIVE_CONNECTIONS = "cloud.oci.load_balancer.active.connections"
LB_ACTIVE_SSL_CONNECTIONS = "cloud.oci.load_balancer.active.ssl.connections"
LB_BYTES_RECEIVED = "cloud.oci.load_balancer.bytes.received"
LB_BYTES_SENT = "cloud.oci.load_balancer.bytes.sent"
LB_ACCEPTED_CONNECTIONS = "cloud.oci.load_balancer.accepted.connections"
LB_HANDLED_CONNECTIONS = "cloud.oci.load_balancer.handled.connections"
LB_FAILED_SSL_HANDSHAKE = "cloud.oci.load_balancer.failed.ssl.handshake"
LB_ACCEPTED_SSL_HANDSHAKE = "cloud.oci.load_balancer.accepted.ssl.handshake"
LB_FAILED_SSL_CLIENT_CERT_VERIFY = (
    "cloud.oci.load_balancer.failed.ssl.client.cert.verify"
)
LB_PEAK_BANDWIDTH = "cloud.oci.load_balancer.peak.bandwidth"
LB_HTTP_RESPONSES = "cloud.oci.load_balancer.http.responses"
LB_HTTP_RESPONSES_200 = "cloud.oci.load_balancer.http.responses.200"
LB_HTTP_RESPONSES_2XX = "cloud.oci.load_balancer.http.responses.2xx"
LB_HTTP_RESPONSES_3XX = "cloud.oci.load_balancer.http.responses.3xx"
LB_HTTP_RESPONSES_4XX = "cloud.oci.load_balancer.http.responses.4xx"
LB_HTTP_RESPONSES_5XX = "cloud.oci.load_balancer.http.responses.5xx"
LB_HTTP_RESPONSES_502 = "cloud.oci.load_balancer.http.responses.502"
LB_HTTP_RESPONSES_504 = "cloud.oci.load_balancer.http.responses.504"
LB_BACKEND_SERVERS = "cloud.oci.load_balancer.backend.servers"
LB_UNHEALTHY_BACKEND_SERVERS = "cloud.oci.load_balancer.unhealthy.backend.servers"
LB_RESPONSE_TIME_HTTP = "cloud.oci.load_balancer.response.time.http"
LB_RESPONSE_TIME_TCP = "cloud.oci.load_balancer.response.time.tcp"
LB_BACKEND_TIMEOUTS = "cloud.oci.load_balancer.backend.timeouts"
LB_INVALID_HEADER_RESPONSES = "cloud.oci.load_balancer.invalid.header.responses"
LB_KEEPALIVE_CONNECTIONS = "cloud.oci.load_balancer.keepalive.connections"
LB_CLOSED_CONNECTIONS = "cloud.oci.load_balancer.closed.connections"

""" All network load balancer metric names imported by extension. """
NETLB_PROCESSED_BYTES = "cloud.oci.network_load_balancer.processed.bytes"
NETLB_PROCESSED_PACKETS = "cloud.oci.network_load_balancer.processed.packets"
NETLB_INGRESS_PACKETS_DROPPED = (
    "cloud.oci.network_load_balancer.ingress.packets.dropped"
)
NETLB_EGRESS_PACKETS_DROPPED = "cloud.oci.network_load_balancer.egress.packets.dropped"
NETLB_HEALTHY_BACKENDS = "cloud.oci.network_load_balancer.healthy.backends"
NETLB_UNHEALTHY_BACKENDS = "cloud.oci.network_load_balancer.unhealthy.backends"
NETLB_NEW_CONNECTIONS = "cloud.oci.network_load_balancer.new.connections"
NETLB_NEW_CONNECTIONS_TCP = "cloud.oci.network_load_balancer.new.connections.tcp"
NETLB_NEW_CONNECTIONS_UDP = "cloud.oci.network_load_balancer.new.connections.udp"

""" All VCN metric names imported by extension. """
VCN_INGRESS_PACKETS_DROPPED_SL = "cloud.oci.vcn.ingress.packets.dropped.sl"
VCN_EGRESS_PACKETS_DROPPED_SL = "cloud.oci.vcn.egress.packets.dropped.sl"
VCN_PACKETS_FROM_NETWORK = "cloud.oci.vcn.packets.from.network"
VCN_PACKETS_TO_NETWORK = "cloud.oci.vcn.packets.to.network"
VCN_BYTES_FROM_NETWORK = "cloud.oci.vcn.bytes.from.network"
VCN_BYTES_TO_NETWORK = "cloud.oci.vcn.bytes.to.network"
VCN_THROTTLED_INGRESS_PACKETS = "cloud.oci.vcn.throttled.ingress.packets"
VCN_THROTTLED_EGRESS_PACKETS = "cloud.oci.vcn.throttled.egress.packets"
VCN_INGRESS_PACKETS_DROPPED_FCTT = "cloud.oci.vcn.ingress.packets.dropped.fctt"
VCN_EGRESS_PACKETS_DROPPED_FCTT = "cloud.oci.vcn.egress.packets.dropped.fctt"
VCN_CTT_UTIL = "cloud.oci.vcn.ctt.util"
VCN_CTT_FULL = "cloud.oci.vcn.ctt.full"

""" All VPN metric names imported by extension. """
VPN_TUNNEL_STATE = "cloud.oci.vpn.tunnel_state"
VPN_PACKETS_RECEIVED = "cloud.oci.vpn.packets_received"
VPN_BYTES_RECEIVED = "cloud.oci.vpn.bytes_received"
VPN_PACKETS_SENT = "cloud.oci.vpn.packets_sent"
VPN_BYTES_SENT = "cloud.oci.vpn.bytes_sent"
VPN_PACKETS_ERROR = "cloud.oci.vpn.packets_error"

""" All block volume metric names imported by extension. """
BLOCKVOL_READ_THROUGHPUT = "cloud.oci.block_volume.read.throughput"
BLOCKVOL_WRITE_THROUGHPUT = "cloud.oci.block_volume.write.throughput"
BLOCKVOL_READ_OPS = "cloud.oci.block_volume.read.ops"
BLOCKVOL_WRITE_OPS = "cloud.oci.block_volume.write.ops"
BLOCKVOL_THROTTLED_IO = "cloud.oci.block_volume.throttled.ops"


""" All object storage metric names imported by extension. """
BUCKET_SIZE = "cloud.oci.object_store.size"
OBJECT_COUNT = "cloud.oci.object_store.count"

""" All instance pool metric names imported by extension. """
INSTANCE_POOL_SIZE = "cloud.oci.instance_pool.size"
INSTANCE_POOL_INSTANCES_PROVISIONING = "cloud.oci.instance_pool.provisioning"
INSTANCE_POOL_INSTANCES_RUNNING = "cloud.oci.instance_pool.running"
INSTANCE_POOL_INSTANCES_TERMINATED = "cloud.oci.instance_pool.terminated"

""" All filesystem metric names imported by extension. """
FS_READ_THROUGHPUT = "cloud.oci.filesystem.read.throughput"
FS_WRITE_THROUGHPUT = "cloud.oci.filesystem.write.throughput"
FS_READ_REQUESTS = "cloud.oci.filesystem.read.requests"
FS_WRITE_REQUESTS = "cloud.oci.filesystem.write.requests"
FS_READ_LATENCY = "cloud.oci.filesystem.read.latency"
FS_WRITE_LATENCY = "cloud.oci.filesystem.write.latency"
FS_METADATA_READ_LATENCY = "cloud.oci.filesystem.metadata.read.latency"
FS_METADATA_WRITE_LATENCY = "cloud.oci.filesystem.metadata.write.latency"
FS_METADATA_IOPS = "cloud.oci.filesystem.metadata.iops"
FS_USAGE = "cloud.oci.filesystem.usage"

""" All API Gateway metric names imported by extension. """
API_GATEWAY_HTTP_REQUESTS = "cloud.oci.api_gateway.http_requests"
API_GATEWAY_HTTP_RESPONSES = "cloud.oci.api_gateway.http_responses"
API_GATEWAY_BYTES_SENT = "cloud.oci.api_gateway.bytes_sent"
API_GATEWAY_BYTES_RECEIVED = "cloud.oci.api_gateway.bytes_received"
API_GATEWAY_LATENCY = "cloud.oci.api_gateway.latency"
API_GATEWAY_INTERNAL_LATENCY = "cloud.oci.api_gateway.internal_latency"
API_GATEWAY_INTEGRATION_LATENCY = "cloud.oci.api_gateway.integration_latency"
API_GATEWAY_BACKEND_HTTP_RESPONSES = "cloud.oci.api_gateway.backend_http_responses"
API_GATEWAY_USAGE_PLAN_REQUESTS = "cloud.oci.api_gateway.usage_plan_requests"
API_GATEWAY_SUBSCRIBER_REQUESTS = "cloud.oci.api_gateway.subscriber_requests"
API_GATEWAY_SUBSCRIBER_QUOTA_PROPORTION_USED = (
    "cloud.oci.api_gateway.subscriber_quota_proportion_used"
)
API_GATEWAY_SUBSCRIBER_RATE_LIMIT_PROPORTION_USED = (
    "cloud.oci.api_gateway.subscriber_rate_limit_proportion_used"
)

""" All Function metric names imported by the extension. """
FUNCTION_INVOCATION_COUNT = "cloud.oci.function.function_invocation_count"
FUNCTION_EXECUTION_DURATION = "cloud.oci.function.function_execution_duration"
FUNCTION_ERROR_RESPONSE_COUNT = "cloud.oci.function.function_error_response_count"
FUNCTION_THROTTLED_RESPONSE_COUNT = (
    "cloud.oci.function.function_throttled_response_count"
)

namespace_map: Dict[str, MetricMapping] = {
    "oci_computeagent": MetricMapping(
        metric_key_map={
            "CpuUtilization": [DynatraceToOCIMetric(COMPUTE_CPU_UTIL, aggregate_max)],
            "DiskBytesRead": [DynatraceToOCIMetric(COMPUTE_DISK_READ_BYTES, aggregate_max)],
            "DiskBytesWritten": [DynatraceToOCIMetric(COMPUTE_DISK_WRITE_BYTES, aggregate_max)],
            "DiskIopsRead": [DynatraceToOCIMetric(COMPUTE_DISK_READ_IO, aggregate_max)],
            "DiskIopsWritten": [DynatraceToOCIMetric(COMPUTE_DISK_WRITE_IO, aggregate_max)],
            "LoadAverage": [DynatraceToOCIMetric(COMPUTE_LOAD, aggregate_max)],
            "MemoryAllocationStalls": [DynatraceToOCIMetric(COMPUTE_MEM_ALLOCATION_STALLS, aggregate_max)],
            "MemoryUtilization": [DynatraceToOCIMetric(COMPUTE_MEM_UTIL, aggregate_max)],
            "NetworksBytesIn": [DynatraceToOCIMetric(COMPUTE_NETWORK_RECEIVE, aggregate_max)],
            "NetworksBytesOut": [DynatraceToOCIMetric(COMPUTE_NETWORK_TRANSMIT, aggregate_max)],
        },
        dimension_map={
            "availabilityDomain": ["oci.availability_domain"],
            "faultDomain": ["oci.fault_domain"],
            "imageId": ["image_id"],
            "instancePoolId": ["instance_pool_id"],
            "region": ["oci.region"],
            "resourceDisplayName": ["oci.resource_display_name"],
            "resourceId": ["oci.resource_id"],
            "resourceGroup": ["oci.resource_group"],
            "compartmentId": ["oci.compartment_id"],
        },
        constant_dimension_map={"cloud.provider": "oci", "oci.service": "compute"},
    ),
    "oci_lbaas": MetricMapping(
        metric_key_map={
            "HttpRequests": [DynatraceToOCIMetric(LB_HTTP_REQUESTS, aggregate_mean)],
            "ActiveConnections": [DynatraceToOCIMetric(LB_ACTIVE_CONNECTIONS, aggregate_mean)],
            "ActiveSSLConnections": [DynatraceToOCIMetric(LB_ACTIVE_SSL_CONNECTIONS, aggregate_mean)],
            "BytesReceived": [DynatraceToOCIMetric(LB_BYTES_RECEIVED, aggregate_mean)],
            "BytesSent": [DynatraceToOCIMetric(LB_BYTES_SENT, aggregate_mean)],
            "AcceptedConnections": [DynatraceToOCIMetric(LB_ACCEPTED_CONNECTIONS, aggregate_mean)],
            "HandledConnections": [DynatraceToOCIMetric(LB_HANDLED_CONNECTIONS, aggregate_mean)],
            "FailedSSLHandshake": [DynatraceToOCIMetric(LB_FAILED_SSL_HANDSHAKE, aggregate_mean)],
            "AcceptedSSLHandshake": [DynatraceToOCIMetric(LB_ACCEPTED_SSL_HANDSHAKE, aggregate_mean)],
            "FailedSSLClientCertVerify": [DynatraceToOCIMetric(
                LB_FAILED_SSL_CLIENT_CERT_VERIFY,
                aggregate_sum,
            )],
            "PeakBandwidth": [DynatraceToOCIMetric(LB_PEAK_BANDWIDTH, aggregate_mean)],
            "HttpResponses": [DynatraceToOCIMetric(LB_HTTP_RESPONSES, aggregate_mean)],
            "HttpResponses200": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_200, aggregate_mean)],
            "HttpResponses2xx": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_2XX, aggregate_mean)],
            "HttpResponses3xx": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_3XX, aggregate_mean)],
            "HttpResponses4xx": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_4XX, aggregate_mean)],
            "HttpResponses5xx": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_5XX, aggregate_mean)],
            "HttpResponses502": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_502, aggregate_mean)],
            "HttpResponses504": [DynatraceToOCIMetric(LB_HTTP_RESPONSES_504, aggregate_mean)],
            "BackendServers": [DynatraceToOCIMetric(LB_BACKEND_SERVERS, aggregate_min)],
            "UnHealthyBackendServers": [DynatraceToOCIMetric(LB_UNHEALTHY_BACKEND_SERVERS, aggregate_max)],
            "ResponseTimeHttpHeader": [DynatraceToOCIMetric(LB_RESPONSE_TIME_HTTP, aggregate_max)],
            "ResponseTimeFirstByte": [DynatraceToOCIMetric(LB_RESPONSE_TIME_TCP, aggregate_max)],
            "BackendTimeouts": [DynatraceToOCIMetric(LB_BACKEND_TIMEOUTS, aggregate_mean)],
            "InvalidHeaderResponses": [DynatraceToOCIMetric(LB_INVALID_HEADER_RESPONSES, aggregate_sum)],
            "KeepaliveConnections": [DynatraceToOCIMetric(LB_KEEPALIVE_CONNECTIONS, aggregate_mean)],
            "ClosedConnections": [DynatraceToOCIMetric(LB_CLOSED_CONNECTIONS, aggregate_sum)],
        },
        dimension_map={
            "availabilityDomain": ["oci.availability_domain"],
            "lbComponent": ["lb_component"],
            "lbHostId": ["lb_host_id"],
            "region": ["oci.region"],
            "lbName": ["oci.resource_display_name"],
            "resourceId": ["oci.resource_id"],
            "resourceGroup": ["oci.resource_group"],
            "compartmentId": ["oci.compartment_id"],
            "backendSetName": ["backend_set_name"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "load_balancer",
        },
    ),
    "oci_nlb": MetricMapping(
        metric_key_map={
            "ProcessedBytes": [DynatraceToOCIMetric(NETLB_PROCESSED_BYTES, aggregate_sum)],
            "ProcessedPackets": [DynatraceToOCIMetric(NETLB_PROCESSED_PACKETS, aggregate_sum)],
            "IngressPacketsDroppedBySL": [DynatraceToOCIMetric(NETLB_INGRESS_PACKETS_DROPPED, aggregate_sum)],
            "EgressPacketsDroppedBySL": [DynatraceToOCIMetric(NETLB_EGRESS_PACKETS_DROPPED, aggregate_sum)],
            "HealthyBackendsPerNlb": [DynatraceToOCIMetric(NETLB_HEALTHY_BACKENDS, aggregate_sum)],
            "UnhealthyBackendsPerNlb": [DynatraceToOCIMetric(NETLB_UNHEALTHY_BACKENDS, aggregate_sum)],
            "NewConnections": [DynatraceToOCIMetric(NETLB_NEW_CONNECTIONS, aggregate_sum)],
            "NewConnectionsTCP": [DynatraceToOCIMetric(NETLB_NEW_CONNECTIONS_TCP, aggregate_sum)],
            "NewConnectionsUDP": [DynatraceToOCIMetric(NETLB_NEW_CONNECTIONS_UDP, aggregate_sum)],
        },
        dimension_map={
            "region": ["oci.region"],
            "resourceName": ["oci.resource_display_name"],
            "resourceId": ["oci.resource_id"],
            "resourceGroup": ["oci.resource_group"],
            "compartmentId": ["oci.compartment_id"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "network_load_balancer",
        },
    ),
    "oci_vcn": MetricMapping(
        metric_key_map={
            "VnicIngressDropsSecurityList": [DynatraceToOCIMetric(
                VCN_INGRESS_PACKETS_DROPPED_SL,
                aggregate_sum,
            )],
            "VnicEgressDropsSecurityList": [DynatraceToOCIMetric(
                VCN_EGRESS_PACKETS_DROPPED_SL,
                aggregate_sum,
            )],
            "VnicFromNetworkPackets": [DynatraceToOCIMetric(VCN_PACKETS_FROM_NETWORK, aggregate_sum)],
            "VnicToNetworkPackets": [DynatraceToOCIMetric(VCN_PACKETS_TO_NETWORK, aggregate_sum)],
            "VnicFromNetworkBytes": [DynatraceToOCIMetric(VCN_BYTES_FROM_NETWORK, aggregate_sum)],
            "VnicToNetworkBytes": [DynatraceToOCIMetric(VCN_BYTES_TO_NETWORK, aggregate_sum)],
            "VnicIngressDropsThrottle": [DynatraceToOCIMetric(VCN_THROTTLED_INGRESS_PACKETS, aggregate_sum)],
            "VnicEgressDropsThrottle": [DynatraceToOCIMetric(VCN_THROTTLED_EGRESS_PACKETS, aggregate_sum)],
            "VnicIngressDropsConntrackFull": [DynatraceToOCIMetric(
                VCN_INGRESS_PACKETS_DROPPED_FCTT,
                aggregate_sum,
            )],
            "VnicEgressDropsConntrackFull": [DynatraceToOCIMetric(
                VCN_EGRESS_PACKETS_DROPPED_FCTT,
                aggregate_sum,
            )],
            "VnicConntrackUtilPercent": [DynatraceToOCIMetric(VCN_CTT_UTIL, aggregate_sum)],
            "VnicConntrackIsFull": [DynatraceToOCIMetric(VCN_CTT_FULL, aggregate_sum)],
        },
        dimension_map={
            "region": ["oci.region"],
            "resourceId": ["oci.resource_id", "oci.resource_display_name","vnic_id"],
            "resourceGroup": ["oci.resource_group"],
            "compartmentId": ["oci.compartment_id"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "vcn",
        },
    ),
    "oci_vpn": MetricMapping(
        metric_key_map={
            "TunnelState": [DynatraceToOCIMetric(VPN_TUNNEL_STATE, aggregate_sum)],
            "PacketsReceived": [DynatraceToOCIMetric(VPN_PACKETS_RECEIVED, aggregate_sum)],
            "BytesReceived": [DynatraceToOCIMetric(VPN_BYTES_RECEIVED, aggregate_sum)],
            "PacketsSent": [DynatraceToOCIMetric(VPN_PACKETS_SENT, aggregate_sum)],
            "BytesSent": [DynatraceToOCIMetric(VPN_BYTES_SENT, aggregate_sum)],
            "PacketsError": [DynatraceToOCIMetric(VPN_PACKETS_ERROR, aggregate_sum)],
        },
        dimension_map={
            "region": ["oci.region"],
            "parentResourceId": ["oci.resource_id"],
            "resourceGroup": ["oci.resource_group"],
            "compartmentId": ["oci.compartment_id"],
            "publicIp": ["vpn.public_ip"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "vpn",
        },
    ),
    "oci_blockstore": MetricMapping(
        metric_key_map={
            "VolumeReadThroughput": [DynatraceToOCIMetric(BLOCKVOL_READ_THROUGHPUT, aggregate_mean)],
            "VolumeWriteThroughput": [DynatraceToOCIMetric(BLOCKVOL_WRITE_THROUGHPUT, aggregate_mean)],
            "VolumeReadOps": [DynatraceToOCIMetric(BLOCKVOL_READ_OPS, aggregate_mean)],
            "VolumeWriteOps": [DynatraceToOCIMetric(BLOCKVOL_WRITE_OPS, aggregate_mean)],
            "VolumeThrottledIOs": [DynatraceToOCIMetric(BLOCKVOL_THROTTLED_IO, aggregate_sum)],
        },
        dimension_map={
            "region": ["oci.region"],
            "resourceId": ["oci.resource_id"],
            "attachmentId": ["attachment_id"],
            "resourceGroup": ["oci.resource_group"],
            "compartmentId": ["oci.compartment_id"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "block_volume",
        },
    ),
    # "oci_objectstore": MetricMapping(
    #     metric_key_map={
    #     'StoredBytes[60m]{tier = "%s"}.sum()': (BUCKET_SIZE),
    #     'ObjectCount[60m]{tier = "%s"}.sum()': (OBJECT_COUNT),
    #     }
    # )
    "oci_instancepools": MetricMapping(
        metric_key_map={
            "InstancePoolSize": [DynatraceToOCIMetric(INSTANCE_POOL_SIZE, aggregate_sum)],
            "ProvisioningInstances": [DynatraceToOCIMetric(
                INSTANCE_POOL_INSTANCES_PROVISIONING,
                aggregate_sum,
            )],
            "RunningInstances": [DynatraceToOCIMetric(INSTANCE_POOL_INSTANCES_RUNNING, aggregate_sum)],
            "TerminatedInstances": [DynatraceToOCIMetric(INSTANCE_POOL_INSTANCES_TERMINATED, aggregate_sum)],
        },
        dimension_map={
            "DisplayName": ["oci.resource_display_name"],
            "region": ["oci.region"],
            "resourceId": ["oci.resource_id"],
            "compartmentId": ["oci.compartment_id"],
            "resourceGroup": ["oci.resource_group"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "instance_pool",
        },
    ),
    "oci_filestorage": MetricMapping(
        metric_key_map={
            'FileSystemReadThroughput': [DynatraceToOCIMetric(FS_READ_THROUGHPUT, aggregate_mean, {"resourceType": "filesystem"})],
            'FileSystemWriteThroughput': [DynatraceToOCIMetric(FS_WRITE_THROUGHPUT, aggregate_mean, {"resourceType": "filesystem"})],
            'FileSystemReadRequestsbySize': [DynatraceToOCIMetric(FS_READ_REQUESTS, aggregate_mean, {"resourceType": "filesystem"})],
            'FileSystemWriteRequestsbySize': [DynatraceToOCIMetric(FS_WRITE_REQUESTS, aggregate_mean, {"resourceType": "filesystem"})],
            'FileSystemReadAverageLatencybySize': [DynatraceToOCIMetric(FS_READ_LATENCY, aggregate_mean, {"resourceType": "filesystem"})],
            'FileSystemWriteAverageLatencybySize': [DynatraceToOCIMetric(FS_WRITE_LATENCY, aggregate_mean, {"resourceType": "filesystem"})],
            'MetadataRequestAverageLatency': [DynatraceToOCIMetric(FS_METADATA_READ_LATENCY, aggregate_mean, {"resourceType": "filesystem", "operation": "ReadMetadata"}), DynatraceToOCIMetric(FS_METADATA_WRITE_LATENCY, aggregate_mean, {"resourceType": "filesystem", "operation": "WriteMetadata"})],
            'MetadataIOPS': [DynatraceToOCIMetric(FS_METADATA_IOPS, aggregate_mean, {"resourceType": "filesystem"})],
            'FileSystemUsage': [DynatraceToOCIMetric(FS_USAGE, aggregate_mean, {"resourceType": "filesystem"})],
        },
        dimension_map={
            "compartmentId": ["oci.compartment_id"],
            "region": ["oci.region"],
            "resourceId": ["oci.resource_id"],
            "resourceGroup": ["oci.resource_group"],
            "resourceType": ["oci.resource_type"],
            "mountTargetId": ["mount_target_id"],
            "throughput": ["throughput"],
            "size": ["size"],
            "operation": ["operation"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "file_system",
        },
    ),
    "oci_apigateway": MetricMapping(
        metric_key_map={
            "HttpRequests": [DynatraceToOCIMetric(API_GATEWAY_HTTP_REQUESTS, aggregate_sum)],
            "HttpResponses": [DynatraceToOCIMetric(API_GATEWAY_HTTP_RESPONSES, aggregate_sum)],
            "BytesSent": [DynatraceToOCIMetric(API_GATEWAY_BYTES_SENT, aggregate_sum)],
            "BytesReceived": [DynatraceToOCIMetric(API_GATEWAY_BYTES_RECEIVED, aggregate_sum)],
            "Latency": [DynatraceToOCIMetric(API_GATEWAY_LATENCY, aggregate_mean)],
            "InternalLatency": [DynatraceToOCIMetric(API_GATEWAY_INTERNAL_LATENCY, aggregate_mean)],
            "IntegrationLatency": [DynatraceToOCIMetric(API_GATEWAY_INTEGRATION_LATENCY, aggregate_mean)],
            "BackendHttpResponses": [DynatraceToOCIMetric(API_GATEWAY_BACKEND_HTTP_RESPONSES, aggregate_sum)],
            "UsagePlanRequests": [DynatraceToOCIMetric(API_GATEWAY_USAGE_PLAN_REQUESTS, aggregate_sum)],
            "SubscriberRequests": [DynatraceToOCIMetric(API_GATEWAY_SUBSCRIBER_REQUESTS, aggregate_sum)],
            "SubscriberQuotaProportionUsed": [DynatraceToOCIMetric(
                API_GATEWAY_SUBSCRIBER_QUOTA_PROPORTION_USED,
                aggregate_mean,
            )],
            "SubscriberRateLimitProportionUsed": [DynatraceToOCIMetric(
                API_GATEWAY_SUBSCRIBER_RATE_LIMIT_PROPORTION_USED,
                aggregate_mean,
            )],
        },
        dimension_map={
            "resourceName": ["oci.resource_display_name"],
            "region": ["oci.region"],
            "resourceId": ["oci.resource_id"],
            "compartmentId": ["oci.compartment_id"],
            "resourceGroup": ["oci.resource_group"],
            "resourceTenantId": ["oci.tenancy_id"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "api_gateway",
        },
    ),
    "oci_faas": MetricMapping(
        metric_key_map={
        "FunctionInvocationCount": [DynatraceToOCIMetric(FUNCTION_INVOCATION_COUNT, aggregate_sum)],
        "FunctionExecutionDuration": [DynatraceToOCIMetric(FUNCTION_EXECUTION_DURATION, aggregate_mean)],
        'FunctionResponseCount': [DynatraceToOCIMetric(FUNCTION_ERROR_RESPONSE_COUNT, aggregate_sum, {"responseType": "Error"}), DynatraceToOCIMetric(FUNCTION_THROTTLED_RESPONSE_COUNT, aggregate_sum, {"responseType": "Throttled"})],
        },
        dimension_map={
            "resourceDisplayName": ["oci.resource_display_name"],
            "region": ["oci.region"],
            "resourceId": ["oci.resource_id"],
            "compartmentId": ["oci.compartment_id"],
            "resourceGroup": ["oci.resource_group"],
            "resourceTenantId": ["oci.tenancy_id"],
        },
        constant_dimension_map={
            "cloud.provider": "oci",
            "oci.service": "function",
        },
    )
}