package com.scylladb.alternator.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class CloseAwareClientProxyTest {

  @Test
  public void testProxyDelegatesMethodsAndClosesOnce() {
    AtomicInteger closeCalls = new AtomicInteger(0);
    SampleClient delegate = new SampleClientImpl();

    SampleClient proxy =
        CloseAwareClientProxy.wrap(SampleClient.class, delegate, closeCalls::incrementAndGet);

    assertEquals("pong", proxy.ping());
    assertSame(delegate, proxy.unwrap());

    proxy.close();
    proxy.close();

    assertEquals(1, closeCalls.get());
  }

  private interface SampleClient extends AutoCloseable {
    String ping();

    SampleClient unwrap();

    @Override
    void close();
  }

  private static final class SampleClientImpl implements SampleClient {
    @Override
    public String ping() {
      return "pong";
    }

    @Override
    public SampleClient unwrap() {
      return this;
    }

    @Override
    public void close() {}
  }
}
