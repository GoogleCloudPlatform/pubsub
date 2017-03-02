package com.google.pubsub.jms.light.message;

import com.google.common.collect.Maps;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Map;

/**
 * Default implementation of property handling methods from {@link javax.jms.Message}.
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractPropertyMessage implements Message, Serializable
{
  private final Map<String, Object> properties = Maps.newHashMap();

  @Override
  public void clearProperties() throws JMSException
  {
    properties.clear();
  }

  @Override
  public boolean propertyExists(final String name) throws JMSException
  {
    return properties.containsKey(name);
  }

  @Override
  public boolean getBooleanProperty(final String name) throws JMSException
  {
    return false;
  }

  @Override
  public byte getByteProperty(final String name) throws JMSException
  {
    return 0;
  }

  // short forced by protocol
  @SuppressWarnings("PMD.AvoidUsingShortType")
  @Override
  public short getShortProperty(final String name) throws JMSException
  {
    return 0;
  }

  @Override
  public int getIntProperty(final String name) throws JMSException
  {
    return 0;
  }

  @Override
  public long getLongProperty(final String name) throws JMSException
  {
    return 0;
  }

  @Override
  public float getFloatProperty(final String name) throws JMSException
  {
    return 0;
  }

  @Override
  public double getDoubleProperty(final String name) throws JMSException
  {
    return 0;
  }

  @Override
  public String getStringProperty(final String name) throws JMSException
  {
    return null;
  }

  @Override
  public Object getObjectProperty(final String name) throws JMSException
  {
    return null;
  }

  @Override
  public Enumeration getPropertyNames() throws JMSException
  {
    return null;
  }

  @Override
  public void setBooleanProperty(final String name, final boolean value) throws JMSException
  {

  }

  @Override
  public void setByteProperty(final String name, final byte value) throws JMSException
  {

  }

  // short forced by protocol
  @SuppressWarnings("PMD.AvoidUsingShortType")
  @Override
  public void setShortProperty(final String name, final short value) throws JMSException
  {

  }

  @Override
  public void setIntProperty(final String name, final int value) throws JMSException
  {

  }

  @Override
  public void setLongProperty(final String name, final long value) throws JMSException
  {

  }

  @Override
  public void setFloatProperty(final String name, final float value) throws JMSException
  {

  }

  @Override
  public void setDoubleProperty(final String name, final double value) throws JMSException
  {

  }

  @Override
  public void setStringProperty(final String name, final String value) throws JMSException
  {

  }

  @Override
  public void setObjectProperty(final String name, final Object value) throws JMSException
  {

  }
}
