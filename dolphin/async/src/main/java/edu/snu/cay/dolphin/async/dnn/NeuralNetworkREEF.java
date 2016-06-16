/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.dnn;

import com.google.common.base.Joiner;
import com.google.protobuf.TextFormat;
import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.dnn.NeuralNetworkParameters.*;
import edu.snu.cay.dolphin.async.dnn.blas.MatrixFactory;
import edu.snu.cay.dolphin.async.dnn.blas.jblas.MatrixJBLASFactory;
import edu.snu.cay.dolphin.async.dnn.conf.*;
import edu.snu.cay.dolphin.async.dnn.data.LayerParameterCodec;
import edu.snu.cay.dolphin.async.dnn.proto.NeuralNetworkProtos.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for the neural network job.
 */
public final class NeuralNetworkREEF {

  private static final Logger LOG = Logger.getLogger(NeuralNetworkREEF.class.getName());

  private final String[] args;
  private final String configurationPath;
  private final boolean onLocal;
  private final String blasLibrary;


  @NamedParameter(doc = "command line arguments")
  private static final class CommandLineArguments implements Name<String> {
  }


  /**
   * @param configurationPath the path for the protobuf configuration file in which a neural network model is defined
   * @param onLocal the flag indicating whether or not to run on local runtime
   * @param blasLibrary the type of BLAS library implementation
   * @param args the command line arguments
   */
  @Inject
  private NeuralNetworkREEF(@Parameter(ConfigurationPath.class) final String configurationPath,
                            @Parameter(OnLocal.class) final boolean onLocal,
                            @Parameter(BlasLibrary.class) final String blasLibrary,
                            @Parameter(CommandLineArguments.class) final String args) {
    this.configurationPath = configurationPath;
    this.onLocal = onLocal;
    this.blasLibrary = blasLibrary;
    this.args = args.split(" ");
  }

  public void run() {
    try {
      final NeuralNetworkConfiguration neuralNetConf = loadNeuralNetworkConfiguration(configurationPath, onLocal);
      final Configuration configuration =
          Configurations.merge(buildNeuralNetworkConfiguration(neuralNetConf), buildBlasConfiguration(blasLibrary));

      AsyncDolphinLauncher.launch("NeuralNetworkREEF", args, AsyncDolphinConfiguration.newBuilder()
          .setWorkerClass(NeuralNetworkWorker.class)
          .setUpdaterClass(NeuralNetworkParameterUpdater.class)
          .setPreValueCodecClass(LayerParameterCodec.class)
          .setValueCodecClass(LayerParameterCodec.class)
          .addParameterClass(Delimiter.class)
          .setWorkerConfiguration(configuration)
          .setServerConfiguration(configuration)
          .build());
    } catch (final IOException e) {
      throw new RuntimeException("Failed to load the protocol buffer definition file for neural network.", e);
    }
  }

  public static NeuralNetworkREEF newInstance(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLineFilter clf = new CommandLineFilter(cb);

    clf.registerShortNameOfClass(OnLocal.class, true);
    clf.registerShortNameOfClass(ConfigurationPath.class);
    clf.registerShortNameOfClass(BlasLibrary.class);

    clf.processCommandLine(args);

    cb.bindNamedParameter(CommandLineArguments.class, Joiner.on(" ").join(clf.getRemainArgs()));

    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(NeuralNetworkREEF.class);
  }

  /**
   * Loads the protocol buffer text formatted neural network configuration
   * from the local filesystem or HDFS depending on {@code onLocal}.
   * @param path the path for the neural network configuration.
   * @param onLocal the flag for the local runtime environment.
   * @return the neural network configuration protocol buffer message.
   * @throws IOException
   */
  private static NeuralNetworkConfiguration loadNeuralNetworkConfiguration(final String path, final boolean onLocal)
      throws IOException {
    final NeuralNetworkConfiguration.Builder neuralNetProtoBuilder = NeuralNetworkConfiguration.newBuilder();

    // Parses neural network builder protobuf message from the prototxt file.
    // Reads from the local filesystem.
    if (onLocal) {
      TextFormat.merge(new FileReader(path), neuralNetProtoBuilder);
      // Reads from HDFS.
    } else {
      final FileSystem fs = FileSystem.get(new JobConf());
      TextFormat.merge(new InputStreamReader(fs.open(new Path(path))), neuralNetProtoBuilder);
    }
    return neuralNetProtoBuilder.build();
  }

  /**
   * Parses the protobuf message and builds neural network configuration.
   * @param neuralNetConf neural network configuration protobuf message.
   * @return the neural network configuration.
   */
  private static Configuration buildNeuralNetworkConfiguration(final NeuralNetworkConfiguration neuralNetConf) {
    final NeuralNetworkConfigurationBuilder neuralNetConfBuilder =
        NeuralNetworkConfigurationBuilder.newConfigurationBuilder();

    neuralNetConfBuilder.setStepSize(neuralNetConf.getStepSize())
        .setInputShape(neuralNetConf.getInputShape().getDimList())
        .setBatchSize(neuralNetConf.getBatchSize());

    // Adds the configuration of each layer.
    for (final LayerConfiguration layerConf : neuralNetConf.getLayerList()) {
      neuralNetConfBuilder.addLayerConfiguration(createLayerConfiguration(layerConf));
    }

    return neuralNetConfBuilder.build();
  }

  /**
   * @param blasLibrary a string that indicates a BLAS library to be used
   * @return the configuration for BLAS library
   */
  private static Configuration buildBlasConfiguration(final String blasLibrary) {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MatrixFactory.class, getMatrixFactoryClass(blasLibrary))
        .build();
  }

  /**
   * @param blasLibraryType a BLAS library string
   * @return the matrix factory class related to the specified BLAS library string
   */
  private static Class<? extends MatrixFactory> getMatrixFactoryClass(final String blasLibraryType) {
    switch (blasLibraryType.toLowerCase()) {
    case "jblas":
      return MatrixJBLASFactory.class;
    default:
      throw new IllegalArgumentException("Unsupported BLAS library: " + blasLibraryType);
    }
  }

  /**
   * Creates the layer configuration from the given protocol buffer layer configuration message.
   * @param layerConf the protocol buffer layer configuration message.
   * @return the layer configuration built from the protocol buffer layer configuration message.
   */
  private static Configuration createLayerConfiguration(final LayerConfiguration layerConf) {
    switch (layerConf.getType().toLowerCase()) {
    case "fullyconnected":
      return FullyConnectedLayerConfigurationBuilder.newConfigurationBuilder()
          .fromProtoConfiguration(layerConf).build();
    case "activation":
      return ActivationLayerConfigurationBuilder.newConfigurationBuilder()
          .fromProtoConfiguration(layerConf).build();
    case "activationwithloss":
      return ActivationWithLossLayerConfigurationBuilder.newConfigurationBuilder()
          .fromProtoConfiguration(layerConf).build();
    case "pooling":
      return PoolingLayerConfigurationBuilder.newConfigurationBuilder()
          .fromProtoConfiguration(layerConf).build();
    case "convolutional":
      return ConvolutionalLayerConfigurationBuilder.newConfigurationBuilder()
          .fromProtoConfiguration(layerConf).build();
    default:
      throw new IllegalArgumentException("Illegal layer type: " + layerConf.getType());
    }
  }

  public static void main(final String[] args) {
    try {
      final NeuralNetworkREEF neuralNetworkREEF = newInstance(args);
      neuralNetworkREEF.run();
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Fatal error occurred:", e);
    }
  }
}