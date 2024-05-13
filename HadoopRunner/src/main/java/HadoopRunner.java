import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;


public class HadoopRunner {
    public static void main(String[] args) {
        if (args.length < 2) {
                System.out.println("Usage: java -jar HadoopRunner-1.jar <arg1> <arg2>");
                System.exit(1);
            }

        String arg1 = args[0];
        String arg2 = args[1];
        
        // AWSCredentials credentials = new BasicSessionCredentials
        //         ("",
        //                 "",
        //                 ""
        //         );
        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://YOURBUCKETNAME/steps-1.0.jar")
                .withMainClass("StepRunner")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data",arg1,arg2);

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("2.7.2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("google 2-gram statistics")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3://YOURBUCKETNAME/logs/")
                .withReleaseLabel("emr-5.0.0");


        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println(" job id: " + jobFlowId);
    }
}
