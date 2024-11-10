package com.kafka.learn.simulator;

import com.kafka.learn.schema.Notification;
import com.kafka.learn.schema.PosInvoice;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class RewardStateProcessor implements ValueTransformer<PosInvoice, Notification> {

  private KeyValueStore<String, Double> stateStore;

  @Override
  public void init(ProcessorContext processorContext) {
    this.stateStore = (KeyValueStore<String, Double>) processorContext.getStateStore(Constants.REWARD_STATE_STORE_NAME);
  }

  @Override
  public Notification transform(PosInvoice posInvoice) {
    Notification notification = new Notification();
    notification.setInvoiceNumber(posInvoice.getInvoiceNumber());
    notification.setCustomerCardNo(posInvoice.getCustomerCardNo());
    notification.setTotalAmount(posInvoice.getTotalAmount());
    notification.setEarnedLoyaltyPoints(posInvoice.getTotalAmount() * Constants.LOYALTY_FACTOR);
    notification.setTotalLoyaltyPoints(0.0);

    Double accumulatedRewardPoints = stateStore.get(posInvoice.getCustomerCardNo().toString());
    Double totalRewards;
    if (accumulatedRewardPoints != null) {
      totalRewards = accumulatedRewardPoints + notification.getEarnedLoyaltyPoints();
    } else {
      totalRewards = notification.getEarnedLoyaltyPoints();
    }
    stateStore.put(posInvoice.getCustomerCardNo().toString(), totalRewards);
    return notification;
  }

  @Override
  public void close() {

  }
}
