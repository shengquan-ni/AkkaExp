package texera.operators.localscan;

import Engine.Common.Constants;
import Engine.Operators.OperatorMetadata;
import Engine.Operators.Scan.LocalFileScan.LocalFileScanMetadata;
import org.hibernate.validator.constraints.NotEmpty;
import texera.common.schema.OperatorGroupConstants;
import texera.common.workflow.TexeraOperator;
import texera.common.schema.TexeraOperatorDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;


public class TexeraLocalFileScan extends TexeraOperator {

    @NotEmpty
    @JsonProperty("file path")
    @JsonPropertyDescription("local file path")
    public String filePath;

    @NotEmpty
    @JsonProperty("delimiter")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @Override
    public OperatorMetadata amberOperator() {
        return new LocalFileScanMetadata(this.amberOperatorTag(), Constants.defaultNumWorkers(),
                filePath, delimiter.charAt(0), null, null);
    }

    @Override
    public TexeraOperatorDescription texeraOperatorDescription() {
        return new TexeraOperatorDescription(
                "Local File Scan",
                "Scan data from a local file",
                OperatorGroupConstants.SOURCE_GROUP(),
                0, 1);
    }

}
