package edu.snu.reef.flexion.examples.ml.algorithms.classification;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.examples.ml.data.LinearModel;
import edu.snu.reef.flexion.examples.ml.data.SGDSummary;
import edu.snu.reef.flexion.examples.ml.parameters.Dimension;
import edu.snu.reef.flexion.examples.ml.parameters.MaxIterations;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import org.apache.mahout.math.DenseVector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogisticRegCtrlTask extends UserControllerTask
    implements DataReduceReceiver<SGDSummary>, DataBroadcastSender<LinearModel> {

  private final static Logger LOG = Logger.getLogger(LogisticRegCtrlTask.class.getName());
  
  private final int maxIter;
  private double lossSum;
  private LinearModel model;


  @Inject
  public LogisticRegCtrlTask(@Parameter(MaxIterations.class) final int maxIter,
                             @Parameter(Dimension.class) final int dimension) {
    this.maxIter = maxIter;

    //randomly initialize model
    this.model = new LinearModel(new DenseVector(dimension+1));

  }
  
  @Override
  public final void run(int iteration) {
    LOG.log(Level.INFO, "{0}-th iteration loss sum: {1}, new model: {2}", new Object[] { iteration, lossSum, model });
  }
  
  @Override
  public final LinearModel sendBroadcastData(int iteration) {
    return model;
  }
  
  @Override
  public final boolean isTerminated(int iteration) {
    return iteration > maxIter;
  }

  @Override
  public void receiveReduceData(SGDSummary sgdSummary) {
    this.lossSum = sgdSummary.getLoss();
    this.model = new LinearModel(sgdSummary.getModel().getParameters().times(1.0/sgdSummary.getCount()));
  }
}
