package edu.snu.reef.elastic.memory.task;

import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.annotations.audience.TaskSide;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@TaskSide
public final class ElasticMemoryServiceClient implements MemoryStoreClient {

  private final Map<String, List> localDataMap;
  private final Map<String, List> elasticDataMap;

  @Inject
  public ElasticMemoryServiceClient() {
    localDataMap = new HashMap<>();
    elasticDataMap = new HashMap<>();
  }

  @Override
  public <T> void putLocal(String key, T value) {
    List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    localDataMap.put(key, singleObjectList);
  }

  @Override
  public <T> void putLocal(String key, List<T> values) {
    localDataMap.put(key, values);
  }

  @Override
  public <T> void putMovable(String key, T value) {
    List<Object> singleObjectList = new LinkedList<>();
    singleObjectList.add(value);
    elasticDataMap.put(key, singleObjectList);
  }

  @Override
  public <T> void putMovable(String key, List<T> values) {
    elasticDataMap.put(key, values);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> get(String key) {
    final List<T> retList = new LinkedList<>();
    if (localDataMap.containsKey(key)) {
      retList.addAll((List<T>)localDataMap.get(key));
    }
    if (elasticDataMap.containsKey(key)) {
      retList.addAll((List<T>)elasticDataMap.get(key));
    }
    return retList;
  }

  @Override
  public void remove(String key) {
    localDataMap.remove(key);
    elasticDataMap.remove(key);
  }

  @Override
  public boolean hasChanged() {
    throw new NotImplementedException();
  }
}
