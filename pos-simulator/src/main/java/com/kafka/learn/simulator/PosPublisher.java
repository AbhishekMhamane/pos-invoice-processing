package com.kafka.learn.simulator;

import com.kafka.learn.schema.MessageKey;
import com.kafka.learn.schema.PosInvoice;
import com.kafka.learn.simulator.datagenerator.InvoiceGenerator;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PosPublisher {

  private static final Logger LOGGER = LoggerFactory.getLogger(PosPublisher.class);

  InvoiceGenerator invoiceGenerator = new InvoiceGenerator();

  private Integer totalNoOfMsg=1;

  private final String interval="2s";

  @Inject
  @Channel("pos-invoice")
  Emitter<Record<MessageKey, PosInvoice>> emitter;

  public void publish(PosInvoice posInvoice){
    MessageKey messageKey = MessageKey.newBuilder().setKey(posInvoice.getCustomerCardNo().toString()).build();
    emitter.send(Record.of(messageKey, posInvoice));
  }

  // Generate random invoices every scheduled intervals
  @Scheduled(every = interval)
  public void generateRandomInvoices(){
    LOGGER.info("Generating random invoices..");

    for(int i=0; i<totalNoOfMsg; i++){
      PosInvoice posInvoice = invoiceGenerator.getInstance().getNextInvoice();
      LOGGER.info("Generated invoice: {}", posInvoice);
      publish(posInvoice);
    }
  }

}
