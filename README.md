In order to create an EC2 cluster on AWS, first create an instance from Amazon Linuc AMI and then do the following on it:

1. sudo yum install git  //installing git
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


 Now save this instance as an image.

 Then, use this image to launch many EC2 instances with all above softwares already installed.