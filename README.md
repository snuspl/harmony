# A scheduling framework for multiple machine learning training jobs running on distributed resources.
  This module includes
   1) a parameter-server (PS) implementation with shared runtime for multiple concurrent jobs and
   2) a long-running master that receives multiple PS job submissions and
   3) a global job scheduler with plugable policy, and
   4) a distributed table abstraction for elastic management of in-memory data across containers.
   
## How to build?
    harmony$ mvn clean install (-DskipTests)
  
### Requirements
  - Java 8 JDK
  - [Apache Maven](https://maven.apache.org/) 3.3 or newer
  - [Flask(python)](http://flask.pocoo.org/): `sudo pip install Flask`.
  - `$ sudo apt-get install libgfortran3` (Ubuntu)
  
## How to run?
    # 1. start a long-running job-server
    harmony$ jobserver/bin/start_jobserver.sh -local false -num_executors 5 -executor_mem_size 128 -executor_num_cores 1
    
    # 2. submit applications (Example usages are described in submit scripts.)
    harmony$ jobserver/bin/submit_[app].sh -input [file_path]
     (Common parameters: -num_mini_batches -max_num_epoch)
     (App-specific parameters: 
        NMF: -rank -step_size -decay_period -decay_rate
        MLR: -init_step_size -classes -features -features_per_partition -model_gaussian -lambda -decay_period -decay_rate
        LDA: -num_topics -num_vocabs
        etc.)
    
    # 3. stop the job-server
    harmony$ jobserver/bin/stop_jobserver.sh


    
