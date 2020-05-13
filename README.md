## In order to create an EC2 cluster on AWS, first create an instance from Amazon Linuc AMI and then do the following on it:
1. sudo yum install git     //installing git
2. Installing JDK from OpenJDK:

       sudo yum install java-1.8.0-openjdk-devel
       sudo update-alternatives --config java
       vim .bashrc     //.bashrc is in the home directory of the user
       Add the following lines to .bashrc:
           export JAVA_HOME=“/usr/lib/jvm/java-1.8.0-openjdk.x86_64”
           PATH=$JAVA_HOME/bin:$PATH
       source .bashrc
 3. Install SBT
 
        curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
        sudo yum install sbt

 4. git clone https://github.com/Hiseen/AkkaExp.git

 Now save this instance as an image. Then, use this image to launch many EC2 instances with all above softwares already installed.


### Local port forwarding to AWS EMR master node: 
       ssh -i <key name>.pem -N -L 8157:127.0.0.1:8088 hadoop@ec2-############.compute.amazonaws.com

## Using Hadoop on AWS EC2: (https://www.novixys.com/blog/setup-apache-hadoop-cluster-aws-ec2/)
1. Install Hadoop (All nodes)

        wget http://apache.mirrors.hoobly.com/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
        tar xvzf hadoop-3.2.1.tar.gz
2. Installing JDK from OpenJDK: (All nodes)

       sudo yum install java-1.8.0-openjdk-devel
       sudo update-alternatives --config java
       vim .bashrc     //.bashrc is in the home directory of the user
       Add the following lines to .bashrc:
           export JAVA_HOME=“/usr/lib/jvm/java-1.8.0-openjdk.x86_64”
           PATH=$JAVA_HOME/bin:$PATH
       source .bashrc
       Also, add JAVA_HOME line to hadoop-3.2.1/etc/hadoop/hadoop-env.sh:
            export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk.x86_64
       Also, add the following to allow hadoop to recognize aws services like S3:
            export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/hadoop-3.2.1/share/hadoop/tools/lib/*
3. Update core-site.xml (hadoop-3.2.1/etc/hadoop/core-site.xml) on all nodes with the name-node address. (All nodes)

        <configuration>
         <property>
           <name>fs.defaultFS</name>
           <value>ec2-#########.us-west-1.compute.amazonaws.com:9000</value>
         </property>
        </configuration>
4. Create the data directory on all nodes and give ownership to ec2-user (All nodes)

        sudo mkdir -p /usr/local/hadoop/hdfs/data
        sudo chown -R ec2-user:ec2-user /usr/local/hadoop/hdfs/data/
        
5. Setup passwordless ssh between name-node and data-nodes.    
        a) ssh-keygen   // do this on name-node and accept empty passphrase.   
        b) After this, take .ssh/id_rsa.pub from the name-node and append it to the .ssh/authorized_keys of name node AND data nodes.   
        c) Then in the name-node's ~/.ssh/config add the following: (Assuming 2 data nodes and 1 name node).   
        
            Host nnode
              HostName ec2-#####.us-west-1.compute.amazonaws.com
              User ec2-user
              IdentityFile ~/.ssh/id_rsa

            Host dnode1
              HostName ec2-#####.us-west-1.compute.amazonaws.com
              User ec2-user
              IdentityFile ~/.ssh/id_rsa

            Host dnode2
              HostName ec2-#####.us-west-1.compute.amazonaws.com
              User ec2-user
              IdentityFile ~/.ssh/id_rsa.   
              
      d) Then, give permissions to ec2-user to use ~/.ssh/config.     
        
              chown $USER ~/.ssh/config
              chmod 644 ~/.ssh/config.  
            
6. Then, in name-node's hadoop-3.2.1/etc/hadoop/hdfs-site.xml, insert the following:

        <configuration>
          <property>
            <name>dfs.replication</name>
            <value>2</value>
          </property>
          <property>
            <name>dfs.namenode.name.dir</name>
            <value>file:///usr/local/hadoop/hdfs/data</value>
          </property>
        </configuration>
   In data-nodes' hadoop-3.2.1/etc/hadoop/hdfs-site.xml, insert the following:
   
          <property>
            <name>dfs.replication</name>
            <value>2</value>
          </property>
          <property>
            <name>dfs.datanode.data.dir</name>
            <value>file:///usr/local/hadoop/hdfs/data</value>
          </property>
7. In name-node's hadoop-3.2.1/etc/hadoop/mapred-site.xml, insert the following:

        <configuration>
          <property>
            <name>mapreduce.jobtracker.address</name>
            <value>ec2-#####.us-west-1.compute.amazonaws.com:54311</value>
          </property>
          <property>
            <name>mapreduce.framework.name</name>
            <value>yarn</value>
          </property>
        </configuration>
8. In name-node's hadoop-3.2.1/etc/hadoop/yarn-site.xml, insert the following:

        <configuration>
        <!-- Site specific YARN configuration properties -->
          <property>
            <name>yarn.nodemanager.aux-services</name>
            <value>mapreduce_shuffle</value>
          </property>
          <property>
            <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
            <value>org.apache.hadoop.mapred.ShuffleHandler</value>
          </property>
          <property>
            <name>yarn.resourcemanager.hostname</name>
            <value>ec2-#####.us-west-1.compute.amazonaws.com</value>
          </property>
        </configuration>
9. In name-node, CREATE a file hadoop-3.2.1/etc/hadoop/masters and put the DNS of name-node in it:   
        ec2-#####.us-west-1.compute.amazonaws.com      
   In name-node, replace all content of hadoop-3.2.1/etc/hadoop/workers with the DNS of data-nodes:     
         ec2-#####.us-west-1.compute.amazonaws.com    
         ec2-#####.us-west-1.compute.amazonaws.com    
10. Starting the hadoop cluster. On name-node, do:

        ./hadoop-3.2.1/sbin/start-dfs.sh
        ./hadoop-3.2.1/sbin/start-yarn.sh
        ./hadoop-3.2.1/sbin/mr-jobhistory-daemon.sh start historyserver
11. The WebUI should be visible at http://<name-node-DNS>:9870
12. To create a folder named `1G` and ingest data:
       
        ./hadoop-3.2.1/bin/hadoop fs -mkdir /1G/
        ./hadoop-3.2.1/bin/hadoop dfs -cp s3://tpch-1g-avi/orders.tbl hdfs://ec2-54-151-35-73.us-west-1.compute.amazonaws.com:9000/1G/orders.tbl

        If EC2 cannot access S3, you should follow steps https://aws.amazon.com/premiumsupport/knowledge-center/ec2-instance-access-s3-bucket/
12. To stop all, Hadoop processes:

        ./hadoop-3.2.1/sbin/stop-all.sh​






