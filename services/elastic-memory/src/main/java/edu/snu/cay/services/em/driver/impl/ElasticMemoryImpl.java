/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.em.driver.impl;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.trace.HTrace;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.LongRange;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@DriverSide
public final class ElasticMemoryImpl implements ElasticMemory {
  private static final String MOVE = "move";

  private final EvaluatorRequestor requestor;
  private final ElasticMemoryMsgSender sender;
  private final ElasticMemoryCallbackRouter callbackRouter;

  private final AtomicLong operatorIdCounter = new AtomicLong();

  @Inject
  private ElasticMemoryImpl(final EvaluatorRequestor requestor,
                            final ElasticMemoryMsgSender sender,
                            final ElasticMemoryCallbackRouter callbackRouter,
                            final HTrace hTrace) {
    hTrace.initialize();
    this.requestor = requestor;
    this.sender = sender;
    this.callbackRouter = callbackRouter;
  }

  @Override
  public void add(final int number, final int megaBytes, final int cores) {
    requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(number)
        .setMemory(megaBytes)
        .setNumberOfCores(cores)
        .build());
  }

  // TODO #112: implement delete
  @Override
  public void delete(final String evalId) {
    throw new NotImplementedException();
  }

  // TODO #113: implement resize
  @Override
  public void resize(final String evalId, final int megaBytes, final int cores) {
    throw new NotImplementedException();
  }

  @Override
  public void move(final String dataType,
                   final Set<LongRange> idRangeSet,
                   final String srcEvalId,
                   final String destEvalId,
                   @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
    try (final TraceScope traceScope = Trace.startSpan(MOVE)) {
      final String operatorId = MOVE + "-" + Long.toString(operatorIdCounter.getAndIncrement());

      callbackRouter.register(operatorId, callback);

      sender.sendCtrlMsg(srcEvalId, dataType, destEvalId, idRangeSet,
          operatorId, TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }

  // TODO #114: implement checkpoint
  @Override
  public void checkpoint(final String evalId) {
    throw new NotImplementedException();
  }
}
