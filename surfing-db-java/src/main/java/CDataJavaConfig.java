import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

/**
 * build target
 */
@Properties(
        target = "CDataJavaToCppExample",
        value = @Platform(
                include = {
                        "/home/chen/surfing-db/src/CDataCppBridge.h"
                },
                compiler = {"cpp17"},
                link = {"arrow"}
        )
)
public class CDataJavaConfig implements InfoMapper {

    @Override
    public void map(InfoMap infoMap) {
        System.out.println(infoMap.toString());
    }
}