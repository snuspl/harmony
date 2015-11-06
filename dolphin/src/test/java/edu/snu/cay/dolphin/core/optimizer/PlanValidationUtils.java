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
package edu.snu.cay.dolphin.core.optimizer;

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Utility class for checking the validity of plans generated by optimizers.
 */
public final class PlanValidationUtils {

  private PlanValidationUtils() {
  }

  /**
   * Checks the validity of the generated plan.
   * @param activeEvaluators the collection of evaluator parameters
   * @param plan the generated plan
   * @param availableEvaluators the number of available evaluators
   */
  public static void checkPlan(final Collection<EvaluatorParameters> activeEvaluators,
                               final Plan plan,
                               final int availableEvaluators) {
    final Collection<EvaluatorParameters> applied = applyPlan(activeEvaluators, plan);
    assertTrue(
        String.format("After optimization, active evaluators %d must be less than or equal to available evaluators %d",
            applied.size(), availableEvaluators),
        applied.size() <= availableEvaluators);
    assertEquals("Data should not be changed", getSumOfDataInfos(activeEvaluators), getSumOfDataInfos(applied));
  }

  /**
   * @param evaluators the collection of evaluator parameters
   * @return a mapping data types to the total number of data units for each data type
   *         in the specified collection of evaluator parameters.
   */
  private static Map<String, Integer> getSumOfDataInfos(final Collection<EvaluatorParameters> evaluators) {
    final Map<String, Integer> dataMap = new HashMap<>();
    for (final EvaluatorParameters evaluator : evaluators) {
      for (final DataInfo dataInfo : evaluator.getDataInfos()) {
        Integer value = dataMap.get(dataInfo.getDataType());
        if (value == null) {
          value = 0;
        }
        dataMap.put(dataInfo.getDataType(), value + dataInfo.getNumUnits());
      }
    }
    return dataMap;
  }

  /**
   * Generates the collection of evaluator parameters after applying the specified plan.
   * Each evaluator id should be unique in the specified collection of evaluator parameters.
   * @param activeEvaluators the collection of evaluator parameters to which the plan is applied
   * @param plan the plan that is applied to the specified collection of evaluator parameters
   * @return the collection of evaluator parameters after applying the specified plan
   */
  private static Collection<EvaluatorParameters> applyPlan(final Collection<EvaluatorParameters> activeEvaluators,
                                                           final Plan plan) {
    final Map<String, EvaluatorParameters> evaluatorMap = new HashMap<>();

    // add existing evaluators
    for (final EvaluatorParameters evaluator : activeEvaluators) {
      assertNull(String.format("Evaluator id %s should be unique", evaluator.getId()),
          evaluatorMap.put(evaluator.getId(), makeCopy(evaluator)));
    }

    // add evaluators generated by the plan
    for (final String idToAdd : plan.getEvaluatorsToAdd()) {
      assertNull(String.format("Evaluator id %s should be unique", idToAdd),
          evaluatorMap.put(idToAdd, getNewEvaluatorParameters(idToAdd)));
    }

    // apply transfer steps generated by the plan
    for (final TransferStep transferStep : plan.getTransferSteps()) {
      final EvaluatorParameters src = evaluatorMap.get(transferStep.getSrcId());
      final EvaluatorParameters dest = evaluatorMap.get(transferStep.getDstId());
      assertNotNull(String.format("Src evaluator %s to transfer data does not exist", transferStep.getSrcId()), src);
      assertNotNull(String.format("Dest evaluator %s to transfer data does not exist", transferStep.getDstId()), dest);

      removeData(src, transferStep.getDataInfo());
      addData(dest, transferStep.getDataInfo());
    }

    // remove evaluators by the plan
    for (final String idToDelete : plan.getEvaluatorsToDelete()) {
      final EvaluatorParameters evaluator = evaluatorMap.remove(idToDelete);
      assertNotNull(String.format("No evaluator whose id is %s", idToDelete), evaluator);
      assertTrue(String.format("Evaluator %s to delete has data", idToDelete), isEmpty(evaluator.getDataInfos()));
    }

    return evaluatorMap.values();
  }

  /**
   * @param id the id used for a new evaluator
   * @return new evaluator parameters
   */
  private static EvaluatorParameters getNewEvaluatorParameters(final String id) {
    return new EvaluatorParametersImpl(id, new ArrayList<DataInfo>(), new HashMap<String, Double>(0));
  }

  /**
   * @param evaluatorParameters the evaluator parameters to copy
   * @return a copy of the specified evaluator parameters
   */
  private static EvaluatorParameters makeCopy(final EvaluatorParameters evaluatorParameters) {
    final Collection<DataInfo> dataInfosCopy = new ArrayList<>(evaluatorParameters.getDataInfos());
    return new EvaluatorParametersImpl(
        evaluatorParameters.getId(), dataInfosCopy, evaluatorParameters.getMetrics());
  }

  /**
   * @param dataInfos the collection of data information
   * @return whether the specified collection of data information is empty or not
   */
  private static boolean isEmpty(final Collection<DataInfo> dataInfos) {
    for (final DataInfo dataInfo : dataInfos) {
      if (dataInfo.getNumUnits() != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Removes the specified data from the evaluator parameters.
   * Assumes that the specified data information is valid.
   * @param evaluator the evaluator parameters from which the specified data is removed
   * @param dataInfoToRemove the information of data to remove
   */
  private static void removeData(final EvaluatorParameters evaluator, final DataInfo dataInfoToRemove) {
    final String id = evaluator.getId();
    final List<DataInfo> dataInfosToRemove = new ArrayList<>(1);

    for (final DataInfo dataInfo : evaluator.getDataInfos()) {
      if (dataInfo.getDataType().equals(dataInfoToRemove.getDataType())) {
        dataInfosToRemove.add(dataInfo);
      }
    }

    assertTrue(String.format("Evaluator %s does not have data for %s", id, dataInfoToRemove.getDataType()),
        dataInfosToRemove.size() > 0);
    assertTrue(String.format("Cannot have multiple data infos for %s", dataInfoToRemove.getDataType()),
        dataInfosToRemove.size() == 1);

    final int numRemainedUnits = dataInfosToRemove.get(0).getNumUnits() - dataInfoToRemove.getNumUnits();
    assertTrue(String.format("Evaluator %s does not have enough data to transfer", id), numRemainedUnits >= 0);

    evaluator.getDataInfos().removeAll(dataInfosToRemove);
    if (numRemainedUnits > 0) {
      evaluator.getDataInfos().add(new DataInfoImpl(dataInfoToRemove.getDataType(), numRemainedUnits));
    }
  }

  /**
   * Adds the specified data to the evaluator parameters.
   * Assumes that the specified data information is valid.
   * @param evaluator the evaluator parameters to which the specified data is added
   * @param dataInfoToAdd the information of data to add
   */
  private static void addData(final EvaluatorParameters evaluator, final DataInfo dataInfoToAdd) {
    final List<DataInfo> dataInfosToAdd = new ArrayList<>(1);

    for (final DataInfo dataInfo : evaluator.getDataInfos()) {
      if (dataInfo.getDataType().equals(dataInfoToAdd.getDataType())) {
        dataInfosToAdd.add(dataInfo);
      }
    }

    assertTrue(String.format("Cannot have multiple data infos for %s", dataInfoToAdd.getDataType()),
        dataInfosToAdd.size() < 2);

    if (dataInfosToAdd.size() == 1) {
      final int numExistingUnits = dataInfosToAdd.get(0).getNumUnits();
      evaluator.getDataInfos().removeAll(dataInfosToAdd);
      evaluator.getDataInfos().add(
          new DataInfoImpl(dataInfoToAdd.getDataType(), numExistingUnits + dataInfoToAdd.getNumUnits()));
    } else {
      evaluator.getDataInfos().add(dataInfoToAdd);
    }
  }
}