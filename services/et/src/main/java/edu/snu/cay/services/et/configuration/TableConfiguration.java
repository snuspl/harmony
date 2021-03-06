/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.configuration.parameters.*;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.evaluator.api.BulkDataLoader;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.*;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.BuilderUtils;

import java.util.Optional;

/**
 * A configuration required for creating a table.
 */
public final class TableConfiguration {
  private final String id;
  private final Class<? extends StreamingCodec> keyCodecClass;
  private final Class<? extends StreamingCodec> valueCodecClass;
  private final Class<? extends Codec> updateValueCodecClass;
  private final Class<? extends UpdateFunction> updateFunctionClass;
  private final boolean isMutableTable;
  private final boolean isOrderedTable;
  private final int chunkSize;
  private final int numTotalBlocks;
  private final Optional<String> chkpPathOptional;
  private final Optional<String> inputPathOptional;
  private final Class<? extends DataParser> dataParserClass;
  private final Class<? extends BulkDataLoader> bulkDataLoaderClass;
  private final Configuration userParamConf;

  private Configuration configuration = null;

  private TableConfiguration(final String id,
                             final Class<? extends StreamingCodec> keyCodecClass,
                             final Class<? extends StreamingCodec> valueCodecClass,
                             final Class<? extends Codec> updateValueCodecClass,
                             final Class<? extends UpdateFunction> updateFunctionClass,
                             final boolean isMutableTable,
                             final boolean isOrderedTable,
                             final Integer chunkSize,
                             final Integer numTotalBlocks,
                             final String chkpPath,
                             final String inputPath,
                             final Class<? extends DataParser> dataParserClass,
                             final Class<? extends BulkDataLoader> bulkDataLoaderClass,
                             final Configuration userParamConf) {
    this.id = id;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
    this.updateValueCodecClass = updateValueCodecClass;
    this.updateFunctionClass = updateFunctionClass;
    this.isMutableTable = isMutableTable;
    this.isOrderedTable = isOrderedTable;
    this.chunkSize = chunkSize;
    this.numTotalBlocks = numTotalBlocks;
    this.chkpPathOptional = Optional.ofNullable(chkpPath);
    this.inputPathOptional = Optional.ofNullable(inputPath);
    this.dataParserClass = dataParserClass;
    this.bulkDataLoaderClass = bulkDataLoaderClass;
    this.userParamConf = userParamConf;
  }

  /**
   * @return a table identifier
   */
  public String getId() {
    return id;
  }

  /**
   * @return a key codec
   */
  public Class<? extends StreamingCodec> getKeyCodecClass() {
    return keyCodecClass;
  }

  /**
   * @return a value codec
   */
  public Class<? extends StreamingCodec> getValueCodecClass() {
    return valueCodecClass;
  }

  /**
   * @return an update value codec
   */
  public Class<? extends Codec> getUpdateValueCodecClass() {
    return updateValueCodecClass;
  }

  /**
   * @return an update function
   */
  public Class<? extends UpdateFunction> getUpdateFunctionClass() {
    return updateFunctionClass;
  }

  /**
   * @return True if it's an mutable table
   */
  public boolean isMutableTable() {
    return isMutableTable;
  }

  /**
   * @return True if it's an ordered table, not a hashed table.
   */
  public boolean isOrderedTable() {
    return isOrderedTable;
  }

  /**
   * @return the number of blocks
   */
  public int getNumTotalBlocks() {
    return numTotalBlocks;
  }

  /**
   * @return the chunk size
   */
  public int getChunkSize() {
    return chunkSize;
  }

  public Optional<String> getChkpPathOptional() {
    return chkpPathOptional;
  }

  public Optional<String> getInputPathOptional() {
    return inputPathOptional;
  }

  /**
   * @return a data parser
   */
  public Class<? extends DataParser> getDataParserClass() {
    return dataParserClass;
  }

  /**
   * @return a bulk data loader
   */
  public Class<? extends BulkDataLoader> getBulkDataLoaderClass() {
    return bulkDataLoaderClass;
  }

  /**
   * @return a user parameter configuration
   */
  public Configuration getUserParamConf() {
    return userParamConf;
  }

  /**
   * @return a tang {@link Configuration} that includes all metadata of table
   */
  public Configuration getConfiguration() {
    if (configuration == null) {
      final Class<? extends BlockPartitioner> blockPartitionerClass = isOrderedTable ?
          OrderingBasedBlockPartitioner.class : HashBasedBlockPartitioner.class;

      configuration = Configurations.merge(
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(TableIdentifier.class, id)
              .bindNamedParameter(KeyCodec.class, keyCodecClass)
              .bindNamedParameter(ValueCodec.class, valueCodecClass)
              .bindNamedParameter(UpdateValueCodec.class, updateValueCodecClass)
              .bindImplementation(UpdateFunction.class, updateFunctionClass)
              .bindNamedParameter(IsMutableTable.class, Boolean.toString(isMutableTable))
              .bindNamedParameter(IsOrderedTable.class, Boolean.toString(isOrderedTable))
              .bindImplementation(BlockPartitioner.class, blockPartitionerClass)
              .bindNamedParameter(ChunkSize.class, Integer.toString(chunkSize))
              .bindNamedParameter(NumTotalBlocks.class, Integer.toString(numTotalBlocks))
              .bindImplementation(DataParser.class, dataParserClass)
              .bindImplementation(BulkDataLoader.class, bulkDataLoaderClass)
              .build(),
          userParamConf);
    }
    return configuration;
  }

  /**
   * @return a builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of TableConfiguration.
   */
  public static final class Builder implements org.apache.reef.util.Builder<TableConfiguration> {
    /**
     * Required parameters.
     */
    private String id;
    private Class<? extends StreamingCodec> keyCodecClass;
    private Class<? extends StreamingCodec> valueCodecClass;
    private Class<? extends Codec> updateValueCodecClass;
    private Class<? extends UpdateFunction> updateFunctionClass;
    private Boolean isMutableTable;
    private Boolean isOrderedTable;

    /**
     * Optional parameters.
     */
    private Integer chunkSize = Integer.valueOf(ChunkSize.DEFAULT_VALUE_STR);
    private Integer numTotalBlocks = Integer.valueOf(NumTotalBlocks.DEFAULT_VALUE_STR);
    private String chkpPath = null;
    private String inputPath = null;
    private Class<? extends DataParser> dataParserClass = DefaultDataParser.class;
    private Class<? extends BulkDataLoader> bulkDataLoaderClass = NoneKeyBulkDataLoader.class;
    private Configuration userParamConf = Tang.Factory.getTang().newConfigurationBuilder().build(); // empty conf

    private Builder() {
    }

    public Builder setId(final String id) {
      this.id = id;
      return this;
    }

    public Builder setKeyCodecClass(final Class<? extends StreamingCodec> keyCodecClass) {
      this.keyCodecClass = keyCodecClass;
      return this;
    }

    public Builder setValueCodecClass(final Class<? extends StreamingCodec> valueCodecClass) {
      this.valueCodecClass = valueCodecClass;
      return this;
    }

    public Builder setUpdateValueCodecClass(final Class<? extends Codec> updateValueCodecClass) {
      this.updateValueCodecClass = updateValueCodecClass;
      return this;
    }

    public Builder setUpdateFunctionClass(final Class<? extends UpdateFunction> updateFunctionClass) {
      this.updateFunctionClass = updateFunctionClass;
      return this;
    }

    public Builder setIsMutableTable(final Boolean isMutableTable) {
      this.isMutableTable = isMutableTable;
      return this;
    }

    public Builder setIsOrderedTable(final Boolean isOrderedTable) {
      this.isOrderedTable = isOrderedTable;
      return this;
    }

    public Builder setChunkSize(final Integer chunkSize) {
      this.chunkSize = chunkSize;
      return this;
    }

    public Builder setNumTotalBlocks(final Integer numTotalBlocks) {
      this.numTotalBlocks = numTotalBlocks;
      return this;
    }

    public Builder setChkpPath(final String chkpPath) {
      this.chkpPath = chkpPath;
      return this;
    }

    public Builder setInputPath(final String inputPath) {
      this.inputPath = inputPath;
      return this;
    }

    public Builder setDataParserClass(final Class<? extends DataParser> dataParserClass) {
      this.dataParserClass = dataParserClass;
      return this;
    }

    public Builder setBulkDataLoaderClass(final Class<? extends BulkDataLoader> bulkDataLoaderClass) {
      this.bulkDataLoaderClass = bulkDataLoaderClass;
      return this;
    }

    public Builder setUserParamConf(final Configuration userParamConf) {
      this.userParamConf = userParamConf;
      return this;
    }

    @Override
    public TableConfiguration build() {
      BuilderUtils.notNull(id);
      BuilderUtils.notNull(keyCodecClass);
      BuilderUtils.notNull(valueCodecClass);
      BuilderUtils.notNull(updateValueCodecClass);
      BuilderUtils.notNull(updateFunctionClass);
      BuilderUtils.notNull(isMutableTable);
      BuilderUtils.notNull(isOrderedTable);

      return new TableConfiguration(id, keyCodecClass, valueCodecClass, updateValueCodecClass, updateFunctionClass,
          isMutableTable, isOrderedTable, chunkSize, numTotalBlocks, chkpPath, inputPath,
          dataParserClass, bulkDataLoaderClass, userParamConf);
    }
  }
}
