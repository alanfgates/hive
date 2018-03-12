/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class NotificationEventsCountRequest implements org.apache.thrift.TBase<NotificationEventsCountRequest, NotificationEventsCountRequest._Fields>, java.io.Serializable, Cloneable, Comparable<NotificationEventsCountRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("NotificationEventsCountRequest");

  private static final org.apache.thrift.protocol.TField FROM_EVENT_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("fromEventId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField CAT_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("catName", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new NotificationEventsCountRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new NotificationEventsCountRequestTupleSchemeFactory());
  }

  private long fromEventId; // required
  private String dbName; // required
  private String catName; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FROM_EVENT_ID((short)1, "fromEventId"),
    DB_NAME((short)2, "dbName"),
    CAT_NAME((short)3, "catName");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FROM_EVENT_ID
          return FROM_EVENT_ID;
        case 2: // DB_NAME
          return DB_NAME;
        case 3: // CAT_NAME
          return CAT_NAME;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __FROMEVENTID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.CAT_NAME};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FROM_EVENT_ID, new org.apache.thrift.meta_data.FieldMetaData("fromEventId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData("dbName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CAT_NAME, new org.apache.thrift.meta_data.FieldMetaData("catName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(NotificationEventsCountRequest.class, metaDataMap);
  }

  public NotificationEventsCountRequest() {
    this.catName = "hive";

  }

  public NotificationEventsCountRequest(
    long fromEventId,
    String dbName)
  {
    this();
    this.fromEventId = fromEventId;
    setFromEventIdIsSet(true);
    this.dbName = dbName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public NotificationEventsCountRequest(NotificationEventsCountRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    this.fromEventId = other.fromEventId;
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetCatName()) {
      this.catName = other.catName;
    }
  }

  public NotificationEventsCountRequest deepCopy() {
    return new NotificationEventsCountRequest(this);
  }

  @Override
  public void clear() {
    setFromEventIdIsSet(false);
    this.fromEventId = 0;
    this.dbName = null;
    this.catName = "hive";

  }

  public long getFromEventId() {
    return this.fromEventId;
  }

  public void setFromEventId(long fromEventId) {
    this.fromEventId = fromEventId;
    setFromEventIdIsSet(true);
  }

  public void unsetFromEventId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __FROMEVENTID_ISSET_ID);
  }

  /** Returns true if field fromEventId is set (has been assigned a value) and false otherwise */
  public boolean isSetFromEventId() {
    return EncodingUtils.testBit(__isset_bitfield, __FROMEVENTID_ISSET_ID);
  }

  public void setFromEventIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __FROMEVENTID_ISSET_ID, value);
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  /** Returns true if field dbName is set (has been assigned a value) and false otherwise */
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
    }
  }

  public String getCatName() {
    return this.catName;
  }

  public void setCatName(String catName) {
    this.catName = catName;
  }

  public void unsetCatName() {
    this.catName = null;
  }

  /** Returns true if field catName is set (has been assigned a value) and false otherwise */
  public boolean isSetCatName() {
    return this.catName != null;
  }

  public void setCatNameIsSet(boolean value) {
    if (!value) {
      this.catName = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FROM_EVENT_ID:
      if (value == null) {
        unsetFromEventId();
      } else {
        setFromEventId((Long)value);
      }
      break;

    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case CAT_NAME:
      if (value == null) {
        unsetCatName();
      } else {
        setCatName((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FROM_EVENT_ID:
      return getFromEventId();

    case DB_NAME:
      return getDbName();

    case CAT_NAME:
      return getCatName();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FROM_EVENT_ID:
      return isSetFromEventId();
    case DB_NAME:
      return isSetDbName();
    case CAT_NAME:
      return isSetCatName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof NotificationEventsCountRequest)
      return this.equals((NotificationEventsCountRequest)that);
    return false;
  }

  public boolean equals(NotificationEventsCountRequest that) {
    if (that == null)
      return false;

    boolean this_present_fromEventId = true;
    boolean that_present_fromEventId = true;
    if (this_present_fromEventId || that_present_fromEventId) {
      if (!(this_present_fromEventId && that_present_fromEventId))
        return false;
      if (this.fromEventId != that.fromEventId)
        return false;
    }

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_catName = true && this.isSetCatName();
    boolean that_present_catName = true && that.isSetCatName();
    if (this_present_catName || that_present_catName) {
      if (!(this_present_catName && that_present_catName))
        return false;
      if (!this.catName.equals(that.catName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_fromEventId = true;
    list.add(present_fromEventId);
    if (present_fromEventId)
      list.add(fromEventId);

    boolean present_dbName = true && (isSetDbName());
    list.add(present_dbName);
    if (present_dbName)
      list.add(dbName);

    boolean present_catName = true && (isSetCatName());
    list.add(present_catName);
    if (present_catName)
      list.add(catName);

    return list.hashCode();
  }

  @Override
  public int compareTo(NotificationEventsCountRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFromEventId()).compareTo(other.isSetFromEventId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFromEventId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.fromEventId, other.fromEventId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDbName()).compareTo(other.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName, other.dbName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCatName()).compareTo(other.isSetCatName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCatName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.catName, other.catName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("NotificationEventsCountRequest(");
    boolean first = true;

    sb.append("fromEventId:");
    sb.append(this.fromEventId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (isSetCatName()) {
      if (!first) sb.append(", ");
      sb.append("catName:");
      if (this.catName == null) {
        sb.append("null");
      } else {
        sb.append(this.catName);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetFromEventId()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'fromEventId' is unset! Struct:" + toString());
    }

    if (!isSetDbName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dbName' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class NotificationEventsCountRequestStandardSchemeFactory implements SchemeFactory {
    public NotificationEventsCountRequestStandardScheme getScheme() {
      return new NotificationEventsCountRequestStandardScheme();
    }
  }

  private static class NotificationEventsCountRequestStandardScheme extends StandardScheme<NotificationEventsCountRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, NotificationEventsCountRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FROM_EVENT_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.fromEventId = iprot.readI64();
              struct.setFromEventIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // DB_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbName = iprot.readString();
              struct.setDbNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CAT_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.catName = iprot.readString();
              struct.setCatNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, NotificationEventsCountRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(FROM_EVENT_ID_FIELD_DESC);
      oprot.writeI64(struct.fromEventId);
      oprot.writeFieldEnd();
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.catName != null) {
        if (struct.isSetCatName()) {
          oprot.writeFieldBegin(CAT_NAME_FIELD_DESC);
          oprot.writeString(struct.catName);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class NotificationEventsCountRequestTupleSchemeFactory implements SchemeFactory {
    public NotificationEventsCountRequestTupleScheme getScheme() {
      return new NotificationEventsCountRequestTupleScheme();
    }
  }

  private static class NotificationEventsCountRequestTupleScheme extends TupleScheme<NotificationEventsCountRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, NotificationEventsCountRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.fromEventId);
      oprot.writeString(struct.dbName);
      BitSet optionals = new BitSet();
      if (struct.isSetCatName()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetCatName()) {
        oprot.writeString(struct.catName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, NotificationEventsCountRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.fromEventId = iprot.readI64();
      struct.setFromEventIdIsSet(true);
      struct.dbName = iprot.readString();
      struct.setDbNameIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.catName = iprot.readString();
        struct.setCatNameIsSet(true);
      }
    }
  }

}

