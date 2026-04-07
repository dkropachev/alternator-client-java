package com.scylladb.alternator.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Creates interface-based client proxies whose {@code close()} method also releases Alternator
 * resources owned outside the AWS SDK client itself.
 */
public final class CloseAwareClientProxy {
  private CloseAwareClientProxy() {}

  /**
   * Wraps a client interface so that invoking {@code close()} runs the provided close action once.
   *
   * @param clientInterface the public client interface to expose
   * @param delegate the underlying SDK client implementation
   * @param closeAction the cleanup action to run when close() is invoked
   * @param <T> client type
   * @return a proxy implementing {@code clientInterface}
   */
  @SuppressWarnings("unchecked")
  public static <T> T wrap(Class<T> clientInterface, T delegate, Runnable closeAction) {
    AtomicBoolean closed = new AtomicBoolean(false);
    InvocationHandler handler =
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
              switch (method.getName()) {
                case "equals":
                  return proxy == args[0];
                case "hashCode":
                  return System.identityHashCode(proxy);
                case "toString":
                  return delegate.toString();
                default:
                  return method.invoke(delegate, args);
              }
            }
            if (method.getName().equals("close") && method.getParameterCount() == 0) {
              if (closed.compareAndSet(false, true)) {
                closeAction.run();
              }
              return null;
            }
            try {
              return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
          }
        };
    return (T)
        Proxy.newProxyInstance(
            clientInterface.getClassLoader(), new Class<?>[] {clientInterface}, handler);
  }
}
