package org.jboss.ddoyle.drools.model;

import java.util.concurrent.TimeUnit;

import org.drools.compiler.CommonTestMethodBase;
import org.drools.compiler.kie.builder.impl.InternalKieModule;
import org.drools.core.common.EventFactHandle;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieModule;
import org.kie.api.builder.ReleaseId;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;


/**
 * Test expiration of events when using inheritance.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class EventExpirationTest extends CommonTestMethodBase {

	/**
	 * Tests the expiration of events when using inheritance.
	 */
	@Test
	public void testEventExpiration() {
		//@formatter:off
		String kmoduleContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
								"<kmodule xmlns=\"http://jboss.org/kie/6.0.0/kmodule\">\n" +
									"<kbase name=\"rules\" equalsBehavior=\"equality\" eventProcessingMode=\"stream\" default=\"true\">\n" +
										"<ksession name=\"ksession-rules\" default=\"true\" type=\"stateful\" clockType=\"pseudo\"/>\n" +
										"</kbase>\n" + 
								"</kmodule>";
		
		
		String drl1 = "package org.jboss.ddoyle.drools.rules\n" +
					"\n" +
					"import org.jboss.ddoyle.drools.model.SimpleEvent\n" +
					"import org.jboss.ddoyle.drools.model.DifferentExpirationSimpleEvent\n" +
					"\n" +
					"declare SimpleEvent\n" +
						"@role( event )\n" +
					    "@timestamp( timestamp )\n" +
						"@expires( 10s )\n" +
					 "end\n" +
					 "\n" +
					 "declare DifferentExpirationSimpleEvent\n" +
					 	"@role (event)\n" +
					 	"@timestamp ( timestamp )\n" +
					 	"@expires ( 100d )\n" +
					 "end\n" +
					 "\n" +
					 "rule \"SimpleEventRule\"\n" +
					 "when\n" +
					 	"$s: SimpleEvent()\n" +
					 "then\n" +
					 	"System.out.println(\"Found SimpleEvent: \" + $s.getId());\n" +
					 "end\n" +
					 "\n" +
					 "rule \"DifferentExpirationSimpleEventRule\"\n" +
					 "when\n" +
					 	"$d: DifferentExpirationSimpleEvent ()\n" +
					    "not DifferentExpirationSimpleEvent (this != $d, this after[0, 30s] $d)\n" +
					 "then\n" +
					     "System.out.println(\"No event within 30 seconds of this event: \" + $d.getId());\n" +
					 "end";
		//@formatter:on

		KieServices ks = KieServices.Factory.get();

		ReleaseId releaseId1 = ks.newReleaseId("org.kie", "test-event-expiration", "1.0.0");
		KieModule km = createAndDeployJar(ks, kmoduleContent, releaseId1, drl1);

		KieContainer kc = ks.newKieContainer(km.getReleaseId());
		KieSession ksession = kc.newKieSession();

		PseudoClockScheduler clock = ksession.getSessionClock();
		DifferentExpirationSimpleEvent event1 = new DifferentExpirationSimpleEvent("1", 0);
		EventFactHandle handle1 = (EventFactHandle) ksession.insert(event1);
		ksession.fireAllRules();

		// Second event added at 31 seconds, so rule should fire for event1.
		DifferentExpirationSimpleEvent event2 = new DifferentExpirationSimpleEvent("2", 31000);
		// Advance the clock to the same time as the event.
		clock.advanceTime(31000, TimeUnit.MILLISECONDS);
		EventFactHandle handle2 = (EventFactHandle) ksession.insert(event2);
		ksession.fireAllRules();

		DifferentExpirationSimpleEvent event3 = new DifferentExpirationSimpleEvent("3", 120000);
		// Advance the clock to the point of the event.
		clock.advanceTime((120000 - 31000), TimeUnit.MILLISECONDS);
		EventFactHandle handle3 = (EventFactHandle) ksession.insert(event3);
		ksession.fireAllRules();

		// All events are of type DifferentExpirationSimpleEvent, which has an expiration of 100 days, so the events should not have
		// expired.

		assertFalse(handle1.isExpired());
		assertFalse(handle2.isExpired());
		assertFalse(handle3.isExpired());

		ksession.dispose();
	}

	/**
	 * Tests the expiration of events when using inheritance.
	 * 
	 * Note that the difference with the previous test is that we insert the event with a given timestamp AFTER the effectiveEnd of the
	 * event. Such an insert traverses a different code-path in PropagationEntry$Insert. I.e. it causes an immediate enqueuing if the
	 * WorkingMemoryReteExpireAction rather than the creation of a scheduled job with a WorkingMemoryReteExpireAction.
	 * 
	 * Actually, the only difference with the previous test is that I don't call 'fireAllRules' after the insertion of event1.
	 */
	@Test
	public void testEventExpiration2() {
		//@formatter:off
		String kmoduleContent = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
								"<kmodule xmlns=\"http://jboss.org/kie/6.0.0/kmodule\">\n" +
									"<kbase name=\"rules\" equalsBehavior=\"equality\" eventProcessingMode=\"stream\" default=\"true\">\n" +
										"<ksession name=\"ksession-rules\" default=\"true\" type=\"stateful\" clockType=\"pseudo\"/>\n" +
										"</kbase>\n" + 
								"</kmodule>";
		
		
		String drl1 = "package org.jboss.ddoyle.drools.rules\n" +
					"\n" +
					"import org.jboss.ddoyle.drools.model.SimpleEvent\n" +
					"import org.jboss.ddoyle.drools.model.DifferentExpirationSimpleEvent\n" +
					"\n" +
					"declare SimpleEvent\n" +
						"@role( event )\n" +
					    "@timestamp( timestamp )\n" +
						"@expires( 10s )\n" +
					 "end\n" +
					 "\n" +
					 "declare DifferentExpirationSimpleEvent\n" +
					 	"@role (event)\n" +
					 	"@timestamp ( timestamp )\n" +
					 	"@expires ( 100d )\n" +
					 "end\n" +
					 "\n" +
					 "rule \"SimpleEventRule\"\n" +
					 "when\n" +
					 	"$s: SimpleEvent()\n" +
					 "then\n" +
					 	"System.out.println(\"Found SimpleEvent: \" + $s.getId());\n" +
					 "end\n" +
					 "\n" +
					 "rule \"DifferentExpirationSimpleEventRule\"\n" +
					 "when\n" +
					 	"$d: DifferentExpirationSimpleEvent ()\n" +
					    "not DifferentExpirationSimpleEvent (this != $d, this after[0, 30s] $d)\n" +
					 "then\n" +
					     "System.out.println(\"No event within 30 seconds of this event: \" + $d.getId());\n" +
					 "end";
		//@formatter:on

		KieServices ks = KieServices.Factory.get();

		ReleaseId releaseId1 = ks.newReleaseId("org.kie", "test-event-expiration", "1.0.0");
		KieModule km = createAndDeployJar(ks, kmoduleContent, releaseId1, drl1);

		KieContainer kc = ks.newKieContainer(km.getReleaseId());
		KieSession ksession = kc.newKieSession();

		PseudoClockScheduler clock = ksession.getSessionClock();
		DifferentExpirationSimpleEvent event1 = new DifferentExpirationSimpleEvent("1", 0);
		EventFactHandle handle1 = (EventFactHandle) ksession.insert(event1);
		
		// Second event added at 31 seconds, so rule should fire for event1.
		DifferentExpirationSimpleEvent event2 = new DifferentExpirationSimpleEvent("2", 31000);
		// Advance the clock to the same time as the event.
		clock.advanceTime(31000, TimeUnit.MILLISECONDS);
		EventFactHandle handle2 = (EventFactHandle) ksession.insert(event2);
		ksession.fireAllRules();

		DifferentExpirationSimpleEvent event3 = new DifferentExpirationSimpleEvent("3", 120000);
		// Advance the clock to the point of the event.
		clock.advanceTime((120000 - 31000), TimeUnit.MILLISECONDS);
		EventFactHandle handle3 = (EventFactHandle) ksession.insert(event3);
		ksession.fireAllRules();

		// All events are of type DifferentExpirationSimpleEvent, which has an expiration of 100 days, so the events should not have
		// expired.

		assertFalse(handle1.isExpired());
		assertFalse(handle2.isExpired());
		assertFalse(handle3.isExpired());

		ksession.dispose();

	}

	/*
	 * @Mario: I added the following method in the test class, because CommenTestMethodBase does not provided a 'createAndDeployJar' that
	 * accepts both String DRLs and kModuleContent. You might want to add these 'createAndDeployJar' and 'createJar' to the
	 * CommonTestMethodBase class.
	 */
	public static KieModule createAndDeployJar(KieServices ks, String kmoduleContent, ReleaseId releaseId, String... drls) {
		byte[] jar = createJar(ks, kmoduleContent, releaseId, drls);

		// Deploy jar into the repository
		KieModule km = deployJarIntoRepository(ks, jar);
		return km;
	}

	public static byte[] createJar(KieServices ks, String kmoduleContent, ReleaseId releaseId, String... drls) {
		KieFileSystem kfs = ks.newKieFileSystem().generateAndWritePomXML(releaseId).writeKModuleXML(kmoduleContent);
		for (int i = 0; i < drls.length; i++) {
			if (drls[i] != null) {
				kfs.write("src/main/resources/r" + i + ".drl", drls[i]);
			}
		}
		KieBuilder kb = ks.newKieBuilder(kfs).buildAll();
		assertFalse(kb.getResults().getMessages(org.kie.api.builder.Message.Level.ERROR).toString(),
				kb.getResults().hasMessages(org.kie.api.builder.Message.Level.ERROR));
		InternalKieModule kieModule = (InternalKieModule) ks.getRepository().getKieModule(releaseId);
		byte[] jar = kieModule.getBytes();
		return jar;
	}

	private static KieModule deployJarIntoRepository(KieServices ks, byte[] jar) {
		Resource jarRes = ks.getResources().newByteArrayResource(jar);
		KieModule km = ks.getRepository().addKieModule(jarRes);
		return km;
	}

}
