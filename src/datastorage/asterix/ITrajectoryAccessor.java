package datastorage.asterix;

import org.json.JSONObject;

import java.util.List;

/**
 * Created by michael on 11/26/16.
 */
public interface ITrajectoryAccessor {
    List<JSONObject> get();

    void execute();

    void insert(List<JSONObject> record);
}
