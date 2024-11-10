package com.kafka.learn.simulator;

import com.kafka.learn.schema.HadoopRecord;
import com.kafka.learn.schema.LineItem;
import com.kafka.learn.schema.Notification;
import com.kafka.learn.schema.PosInvoice;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class RecordBuilder {

  public List<HadoopRecord> getHadoopRecords(PosInvoice invoice) {
    List<HadoopRecord> records = new ArrayList<>();
    for (LineItem i : invoice.getInvoiceLineItems()) {
      HadoopRecord record = new HadoopRecord();
      record.setInvoiceNumber(invoice.getInvoiceNumber());
      record.setCreatedTime(invoice.getCreatedTime());
      record.setStoreID(invoice.getStoreID());
      record.setPosID(invoice.getPosID());
      record.setCustomerType(invoice.getCustomerType());
      record.setPaymentMethod(invoice.getPaymentMethod());
      record.setDeliveryType(invoice.getDeliveryType());
      record.setItemCode(i.getItemCode());
      record.setItemDescription(i.getItemDescription());
      record.setItemPrice(i.getItemPrice());
      record.setItemQty(i.getItemQty());
      record.setTotalValue(i.getTotalValue());
      if (invoice.getDeliveryType().toString().equalsIgnoreCase(Constants.DELIVERY_TYPE_HOME_DELIVERY)) {
        record.setCity(invoice.getDeliveryAddress().getCity());
        record.setState(invoice.getDeliveryAddress().getState());
        record.setPinCode(invoice.getDeliveryAddress().getPinCode());
      }
      records.add(record);
    }
    return records;
  }

  /**
   * Set personally identifiable values to null
   *
   * @param invoice PosInvoice object
   * @return masked PosInvoice object
   */
  public PosInvoice getMaskedInvoice(PosInvoice invoice) {
    invoice.setCustomerCardNo(null);
    if (invoice.getDeliveryType().toString().equalsIgnoreCase(Constants.DELIVERY_TYPE_HOME_DELIVERY)) {
      invoice.getDeliveryAddress().setAddressLine(null);
      invoice.getDeliveryAddress().setContactNumber(null);
    }
    return invoice;
  }

  /**
   * Transform PosInvoice to Notification
   *
   * @param invoice PosInvoice Object
   * @return Notification Object
   */
  public Notification getNotification(PosInvoice invoice) {
    Notification notification =  new Notification();
    notification.setInvoiceNumber(invoice.getInvoiceNumber());
    notification.setCustomerCardNo(invoice.getCustomerCardNo());
    notification.setTotalAmount(invoice.getTotalAmount());
    notification.setEarnedLoyaltyPoints(invoice.getTotalAmount() * Constants.LOYALTY_FACTOR);
    return notification;
  }
}
