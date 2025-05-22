/*
 * Copyright 2024 - 2025 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Port configuration for protocol services in the Traccar microservices architecture.
 * Supports dynamic port assignment, containerized environments, and service mesh integration.
 */
public class PortConfigSuffix extends ConfigSuffix<Integer> {
    
    private static final Logger LOGGER = Logger.getLogger(PortConfigSuffix.class.getName());
    
    // Environment variable prefixes for port configuration
    private static final String ENV_PORT_PREFIX = "TRACCAR_PORT_";
    private static final String ENV_SERVICE_PREFIX = "TRACCAR_SERVICE_";
    private static final String ENV_K8S_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";
    private static final String ENV_PORT_OFFSET = "TRACCAR_PORT_OFFSET";
    
    // Default port offset for multi-service deployments to avoid conflicts
    private static final int DEFAULT_PORT_OFFSET = 0;
    
    // Service types for port allocation
    public enum ServiceType {
        PROTOCOL,      // Protocol handling service
        POSITION,      // Position processing service
        EVENT,         // Event processing service
        NOTIFICATION,  // Notification service
        API_GATEWAY,   // API Gateway service
        REPORTING      // Reporting service
    }
    
    // Current service type, defaults to PROTOCOL
    private static ServiceType currentServiceType = ServiceType.PROTOCOL;
    
    // Port offset for multi-service deployments
    private static int portOffset = DEFAULT_PORT_OFFSET;
    
    // Flag to indicate if running in Kubernetes
    private static boolean isKubernetesEnvironment = false;
    
    // Cache for dynamically resolved ports
    private static final Map<String, Integer> resolvedPorts = new ConcurrentHashMap<>();
    
    // Default port mappings
    private static final Map<String, Integer> DEFAULT_PORTS = new HashMap<>();
    
    static {
        DEFAULT_PORTS.put("gps103", 5001);
        DEFAULT_PORTS.put("tk103", 5002);
        DEFAULT_PORTS.put("gl100", 5003);
        DEFAULT_PORTS.put("gl200", 5004);
        DEFAULT_PORTS.put("t55", 5005);
        DEFAULT_PORTS.put("xexun", 5006);
        DEFAULT_PORTS.put("totem", 5007);
        DEFAULT_PORTS.put("enfora", 5008);
        DEFAULT_PORTS.put("meiligao", 5009);
        DEFAULT_PORTS.put("trv", 5010);
        DEFAULT_PORTS.put("suntech", 5011);
        DEFAULT_PORTS.put("progress", 5012);
        DEFAULT_PORTS.put("h02", 5013);
        DEFAULT_PORTS.put("jt600", 5014);
        DEFAULT_PORTS.put("huabao", 5015);
        DEFAULT_PORTS.put("v680", 5016);
        DEFAULT_PORTS.put("pt502", 5017);
        DEFAULT_PORTS.put("tr20", 5018);
        DEFAULT_PORTS.put("navis", 5019);
        DEFAULT_PORTS.put("meitrack", 5020);
        DEFAULT_PORTS.put("skypatrol", 5021);
        DEFAULT_PORTS.put("gt02", 5022);
        DEFAULT_PORTS.put("gt06", 5023);
        DEFAULT_PORTS.put("megastek", 5024);
        DEFAULT_PORTS.put("navigil", 5025);
        DEFAULT_PORTS.put("gpsgate", 5026);
        DEFAULT_PORTS.put("teltonika", 5027);
        DEFAULT_PORTS.put("mta6", 5028);
        DEFAULT_PORTS.put("tzone", 5029);
        DEFAULT_PORTS.put("tlt2h", 5030);
        DEFAULT_PORTS.put("taip", 5031);
        DEFAULT_PORTS.put("wondex", 5032);
        DEFAULT_PORTS.put("cellocator", 5033);
        DEFAULT_PORTS.put("galileo", 5034);
        DEFAULT_PORTS.put("ywt", 5035);
        DEFAULT_PORTS.put("tk102", 5036);
        DEFAULT_PORTS.put("intellitrac", 5037);
        DEFAULT_PORTS.put("gpsmta", 5038);
        DEFAULT_PORTS.put("wialon", 5039);
        DEFAULT_PORTS.put("carscop", 5040);
        DEFAULT_PORTS.put("apel", 5041);
        DEFAULT_PORTS.put("manpower", 5042);
        DEFAULT_PORTS.put("globalsat", 5043);
        DEFAULT_PORTS.put("atrack", 5044);
        DEFAULT_PORTS.put("pt3000", 5045);
        DEFAULT_PORTS.put("ruptela", 5046);
        DEFAULT_PORTS.put("topflytech", 5047);
        DEFAULT_PORTS.put("laipac", 5048);
        DEFAULT_PORTS.put("aplicom", 5049);
        DEFAULT_PORTS.put("gotop", 5050);
        DEFAULT_PORTS.put("sanav", 5051);
        DEFAULT_PORTS.put("gator", 5052);
        DEFAULT_PORTS.put("noran", 5053);
        DEFAULT_PORTS.put("m2m", 5054);
        DEFAULT_PORTS.put("osmand", 5055);
        DEFAULT_PORTS.put("easytrack", 5056);
        DEFAULT_PORTS.put("gpsmarker", 5057);
        DEFAULT_PORTS.put("khd", 5058);
        DEFAULT_PORTS.put("piligrim", 5059);
        DEFAULT_PORTS.put("stl060", 5060);
        DEFAULT_PORTS.put("cartrack", 5061);
        DEFAULT_PORTS.put("minifinder", 5062);
        DEFAULT_PORTS.put("haicom", 5063);
        DEFAULT_PORTS.put("eelink", 5064);
        DEFAULT_PORTS.put("box", 5065);
        DEFAULT_PORTS.put("freedom", 5066);
        DEFAULT_PORTS.put("telic", 5067);
        DEFAULT_PORTS.put("trackbox", 5068);
        DEFAULT_PORTS.put("visiontek", 5069);
        DEFAULT_PORTS.put("orion", 5070);
        DEFAULT_PORTS.put("riti", 5071);
        DEFAULT_PORTS.put("ulbotech", 5072);
        DEFAULT_PORTS.put("tramigo", 5073);
        DEFAULT_PORTS.put("tr900", 5074);
        DEFAULT_PORTS.put("ardi01", 5075);
        DEFAULT_PORTS.put("xt013", 5076);
        DEFAULT_PORTS.put("autofon", 5077);
        DEFAULT_PORTS.put("gosafe", 5078);
        DEFAULT_PORTS.put("tt8850", 5079);
        DEFAULT_PORTS.put("bce", 5080);
        DEFAULT_PORTS.put("xirgo", 5081);
        DEFAULT_PORTS.put("calamp", 5082);
        DEFAULT_PORTS.put("mtx", 5083);
        DEFAULT_PORTS.put("tytan", 5084);
        DEFAULT_PORTS.put("avl301", 5085);
        DEFAULT_PORTS.put("castel", 5086);
        DEFAULT_PORTS.put("mxt", 5087);
        DEFAULT_PORTS.put("cityeasy", 5088);
        DEFAULT_PORTS.put("aquila", 5089);
        DEFAULT_PORTS.put("flextrack", 5090);
        DEFAULT_PORTS.put("blackkite", 5091);
        DEFAULT_PORTS.put("adm", 5092);
        DEFAULT_PORTS.put("watch", 5093);
        DEFAULT_PORTS.put("t800x", 5094);
        DEFAULT_PORTS.put("upro", 5095);
        DEFAULT_PORTS.put("auro", 5096);
        DEFAULT_PORTS.put("disha", 5097);
        DEFAULT_PORTS.put("thinkrace", 5098);
        DEFAULT_PORTS.put("pathaway", 5099);
        DEFAULT_PORTS.put("arnavi", 5100);
        DEFAULT_PORTS.put("nvs", 5101);
        DEFAULT_PORTS.put("kenji", 5102);
        DEFAULT_PORTS.put("astra", 5103);
        DEFAULT_PORTS.put("homtecs", 5104);
        DEFAULT_PORTS.put("fox", 5105);
        DEFAULT_PORTS.put("gnx", 5106);
        DEFAULT_PORTS.put("arknav", 5107);
        DEFAULT_PORTS.put("supermate", 5108);
        DEFAULT_PORTS.put("appello", 5109);
        DEFAULT_PORTS.put("idpl", 5110);
        DEFAULT_PORTS.put("huasheng", 5111);
        DEFAULT_PORTS.put("l100", 5112);
        DEFAULT_PORTS.put("granit", 5113);
        DEFAULT_PORTS.put("carcell", 5114);
        DEFAULT_PORTS.put("obddongle", 5115);
        DEFAULT_PORTS.put("hunterpro", 5116);
        DEFAULT_PORTS.put("raveon", 5117);
        DEFAULT_PORTS.put("cradlepoint", 5118);
        DEFAULT_PORTS.put("arknavx8", 5119);
        DEFAULT_PORTS.put("autograde", 5120);
        DEFAULT_PORTS.put("oigo", 5121);
        DEFAULT_PORTS.put("jpkorjar", 5122);
        DEFAULT_PORTS.put("cguard", 5123);
        DEFAULT_PORTS.put("fifotrack", 5124);
        DEFAULT_PORTS.put("smokey", 5125);
        DEFAULT_PORTS.put("extremtrac", 5126);
        DEFAULT_PORTS.put("trakmate", 5127);
        DEFAULT_PORTS.put("at2000", 5128);
        DEFAULT_PORTS.put("maestro", 5129);
        DEFAULT_PORTS.put("ais", 5130);
        DEFAULT_PORTS.put("gt30", 5131);
        DEFAULT_PORTS.put("tmg", 5132);
        DEFAULT_PORTS.put("pretrace", 5133);
        DEFAULT_PORTS.put("pricol", 5134);
        DEFAULT_PORTS.put("siwi", 5135);
        DEFAULT_PORTS.put("starlink", 5136);
        DEFAULT_PORTS.put("dmt", 5137);
        DEFAULT_PORTS.put("xt2400", 5138);
        DEFAULT_PORTS.put("dmthttp", 5139);
        DEFAULT_PORTS.put("alematics", 5140);
        DEFAULT_PORTS.put("gps056", 5141);
        DEFAULT_PORTS.put("flexcomm", 5142);
        DEFAULT_PORTS.put("vt200", 5143);
        DEFAULT_PORTS.put("owntracks", 5144);
        DEFAULT_PORTS.put("vtfms", 5145);
        DEFAULT_PORTS.put("tlv", 5146);
        DEFAULT_PORTS.put("esky", 5147);
        DEFAULT_PORTS.put("genx", 5148);
        DEFAULT_PORTS.put("flespi", 5149);
        DEFAULT_PORTS.put("dway", 5150);
        DEFAULT_PORTS.put("recoda", 5151);
        DEFAULT_PORTS.put("oko", 5152);
        DEFAULT_PORTS.put("ivt401", 5153);
        DEFAULT_PORTS.put("sigfox", 5154);
        DEFAULT_PORTS.put("t57", 5155);
        DEFAULT_PORTS.put("spot", 5156);
        DEFAULT_PORTS.put("m2c", 5157);
        DEFAULT_PORTS.put("austinnb", 5158);
        DEFAULT_PORTS.put("opengts", 5159);
        DEFAULT_PORTS.put("cautela", 5160);
        DEFAULT_PORTS.put("continental", 5161);
        DEFAULT_PORTS.put("egts", 5162);
        DEFAULT_PORTS.put("robotrack", 5163);
        DEFAULT_PORTS.put("pt60", 5164);
        DEFAULT_PORTS.put("telemax", 5165);
        DEFAULT_PORTS.put("sabertek", 5166);
        DEFAULT_PORTS.put("retranslator", 5167);
        DEFAULT_PORTS.put("svias", 5168);
        DEFAULT_PORTS.put("eseal", 5169);
        DEFAULT_PORTS.put("freematics", 5170);
        DEFAULT_PORTS.put("avema", 5171);
        DEFAULT_PORTS.put("autotrack", 5172);
        DEFAULT_PORTS.put("tek", 5173);
        DEFAULT_PORTS.put("wristband", 5174);
        DEFAULT_PORTS.put("milesmate", 5176);
        DEFAULT_PORTS.put("anytrek", 5177);
        DEFAULT_PORTS.put("smartsole", 5178);
        DEFAULT_PORTS.put("its", 5179);
        DEFAULT_PORTS.put("xrb28", 5180);
        DEFAULT_PORTS.put("c2stek", 5181);
        DEFAULT_PORTS.put("nyitech", 5182);
        DEFAULT_PORTS.put("neos", 5183);
        DEFAULT_PORTS.put("satsol", 5184);
        DEFAULT_PORTS.put("globalstar", 5185);
        DEFAULT_PORTS.put("sanul", 5186);
        DEFAULT_PORTS.put("minifinder2", 5187);
        DEFAULT_PORTS.put("radar", 5188);
        DEFAULT_PORTS.put("techtlt", 5189);
        DEFAULT_PORTS.put("starcom", 5190);
        DEFAULT_PORTS.put("mictrack", 5191);
        DEFAULT_PORTS.put("plugin", 5192);
        DEFAULT_PORTS.put("leafspy", 5193);
        DEFAULT_PORTS.put("naviset", 5194);
        DEFAULT_PORTS.put("racedynamics", 5195);
        DEFAULT_PORTS.put("rst", 5196);
        DEFAULT_PORTS.put("pt215", 5197);
        DEFAULT_PORTS.put("pacifictrack", 5198);
        DEFAULT_PORTS.put("topin", 5199);
        DEFAULT_PORTS.put("outsafe", 5200);
        DEFAULT_PORTS.put("solarpowered", 5201);
        DEFAULT_PORTS.put("motor", 5202);
        DEFAULT_PORTS.put("omnicomm", 5203);
        DEFAULT_PORTS.put("s168", 5204);
        DEFAULT_PORTS.put("vnet", 5205);
        DEFAULT_PORTS.put("blue", 5206);
        DEFAULT_PORTS.put("pst", 5207);
        DEFAULT_PORTS.put("dingtek", 5208);
        DEFAULT_PORTS.put("wli", 5209);
        DEFAULT_PORTS.put("niot", 5210);
        DEFAULT_PORTS.put("portman", 5211);
        DEFAULT_PORTS.put("moovbox", 5212);
        DEFAULT_PORTS.put("futureway", 5213);
        DEFAULT_PORTS.put("polte", 5214);
        DEFAULT_PORTS.put("net", 5215);
        DEFAULT_PORTS.put("mobilogix", 5216);
        DEFAULT_PORTS.put("swiftech", 5217);
        DEFAULT_PORTS.put("iotm", 5218);
        DEFAULT_PORTS.put("dolphin", 5219);
        DEFAULT_PORTS.put("ennfu", 5220);
        DEFAULT_PORTS.put("navtelecom", 5221);
        DEFAULT_PORTS.put("startek", 5222);
        DEFAULT_PORTS.put("gs100", 5223);
        DEFAULT_PORTS.put("mavlink2", 5224);
        DEFAULT_PORTS.put("uux", 5225);
        DEFAULT_PORTS.put("r12w", 5226);
        DEFAULT_PORTS.put("flexiblereport", 5227);
        DEFAULT_PORTS.put("thinkpower", 5228);
        DEFAULT_PORTS.put("stb", 5229);
        DEFAULT_PORTS.put("b2316", 5230);
        DEFAULT_PORTS.put("hoopo", 5231);
        DEFAULT_PORTS.put("dualcam", 5232);
        DEFAULT_PORTS.put("xexun2", 5233);
        DEFAULT_PORTS.put("techtocruz", 5234);
        DEFAULT_PORTS.put("flexapi", 5235);
        DEFAULT_PORTS.put("dsf22", 5236);
        DEFAULT_PORTS.put("jido", 5237);
        DEFAULT_PORTS.put("armoli", 5238);
        DEFAULT_PORTS.put("teratrack", 5239);
        DEFAULT_PORTS.put("envotech", 5240);
        DEFAULT_PORTS.put("bstpl", 5241);
        DEFAULT_PORTS.put("thuraya", 5242);
        DEFAULT_PORTS.put("ndtpv6", 5243);
        DEFAULT_PORTS.put("g1rus", 5244);
        DEFAULT_PORTS.put("rftrack", 5245);
        DEFAULT_PORTS.put("vlt", 5246);
        DEFAULT_PORTS.put("transync", 5247);
        DEFAULT_PORTS.put("t622iridium", 5248);
        DEFAULT_PORTS.put("pui", 5249);
        DEFAULT_PORTS.put("nto", 5250);
        DEFAULT_PORTS.put("ramac", 5251);
        DEFAULT_PORTS.put("positrex", 5252);
        DEFAULT_PORTS.put("dragino", 5253);
        DEFAULT_PORTS.put("fleetguide", 5254);
        DEFAULT_PORTS.put("valtrack", 5255);
        DEFAULT_PORTS.put("snapper", 5256);
        DEFAULT_PORTS.put("gl601", 5257);
        
        // Initialize environment detection
        initializeEnvironment();
    }
    
    /**
     * Initialize environment detection for containerized deployments.
     * Detects Kubernetes environment and sets up port offset for conflict resolution.
     */
    private static void initializeEnvironment() {
        // Check if running in Kubernetes
        String k8sHost = System.getenv(ENV_K8S_SERVICE_HOST);
        isKubernetesEnvironment = k8sHost != null && !k8sHost.isEmpty();
        
        // Get service type from environment if available
        String serviceTypeEnv = System.getenv(ENV_SERVICE_PREFIX + "TYPE");
        if (serviceTypeEnv != null && !serviceTypeEnv.isEmpty()) {
            try {
                currentServiceType = ServiceType.valueOf(serviceTypeEnv.toUpperCase());
                LOGGER.info("Service type set to: " + currentServiceType);
            } catch (IllegalArgumentException e) {
                LOGGER.warning("Invalid service type: " + serviceTypeEnv + ". Using default: " + currentServiceType);
            }
        }
        
        // Get port offset from environment if available
        String offsetEnv = System.getenv(ENV_PORT_OFFSET);
        if (offsetEnv != null && !offsetEnv.isEmpty()) {
            try {
                portOffset = Integer.parseInt(offsetEnv);
                LOGGER.info("Port offset set to: " + portOffset);
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid port offset: " + offsetEnv + ". Using default: " + DEFAULT_PORT_OFFSET);
            }
        }
        
        if (isKubernetesEnvironment) {
            LOGGER.info("Running in Kubernetes environment");
        }
    }

    /**
     * Set the current service type.
     * This affects how ports are allocated and resolved.
     * 
     * @param serviceType The service type to set
     */
    public static void setServiceType(ServiceType serviceType) {
        currentServiceType = serviceType;
        LOGGER.info("Service type set to: " + serviceType);
    }
    
    /**
     * Set the port offset for multi-service deployments.
     * This helps avoid port conflicts when multiple services are deployed.
     * 
     * @param offset The port offset to apply
     */
    public static void setPortOffset(int offset) {
        portOffset = offset;
        LOGGER.info("Port offset set to: " + offset);
        // Clear resolved ports cache when offset changes
        resolvedPorts.clear();
    }
    
    /**
     * Get the resolved port for a protocol, taking into account environment variables,
     * service type, port offset, and containerization.
     * 
     * @param protocol The protocol name
     * @return The resolved port number
     */
    public static int getResolvedPort(String protocol) {
        // Check cache first
        if (resolvedPorts.containsKey(protocol)) {
            return resolvedPorts.get(protocol);
        }
        
        // Try to get from environment variable
        String envVarName = ENV_PORT_PREFIX + protocol.toUpperCase();
        String envPort = System.getenv(envVarName);
        
        if (envPort != null && !envPort.isEmpty()) {
            try {
                int port = Integer.parseInt(envPort);
                resolvedPorts.put(protocol, port);
                LOGGER.info("Using environment-configured port for " + protocol + ": " + port);
                return port;
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid port in environment variable " + envVarName + ": " + envPort);
            }
        }
        
        // Get default port and apply offset
        Integer defaultPort = DEFAULT_PORTS.get(protocol);
        if (defaultPort == null) {
            LOGGER.warning("No default port defined for protocol: " + protocol);
            defaultPort = 5000; // Fallback default
        }
        
        // Apply port offset for multi-service deployments
        int resolvedPort = defaultPort + portOffset;
        
        // In Kubernetes with service mesh, we might need to adjust ports
        if (isKubernetesEnvironment && currentServiceType != ServiceType.PROTOCOL) {
            // In non-protocol services, we might need special handling
            // This is a placeholder for service mesh integration
            LOGGER.info("Service mesh integration active for " + protocol + " in " + currentServiceType);
        }
        
        // Cache the resolved port
        resolvedPorts.put(protocol, resolvedPort);
        return resolvedPort;
    }

    PortConfigSuffix(String key, List<KeyType> types) {
        super(key, types, null);
    }

    @Override
    public ConfigKey<Integer> withPrefix(String protocol) {
        // Use the dynamic port resolution system
        int port = getResolvedPort(protocol);
        return new IntegerConfigKey(protocol + keySuffix, types, port);
    }
}