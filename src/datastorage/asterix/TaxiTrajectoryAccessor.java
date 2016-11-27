package datastorage.asterix;

import gov.nasa.worldwind.exception.ServiceException;
import org.apache.asterix.AsterixConf;
import org.apache.asterix.AsterixConn;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by michael on 11/26/16.
 */
public class TaxiTrajectoryAccessor implements ITrajectoryAccessor {
    private String query = null;
    AsterixConf conf = new AsterixConf();
    AsterixConn conn = new AsterixConn(null, null);
    AsterixConf.OpType opType = AsterixConf.OpType.QUERY;
    String result = null;

    public void init(String sql) {
        this.query = sql;
        conf.setDataverse("gps");
    }

    public TaxiTrajectoryAccessor(int id) {
        String sql = "for $d in dataset taxiTrajectory; where $d.id = " + id + "; order by $d.at; return $d";
        init(sql);
    }

    public TaxiTrajectoryAccessor(int id, LocalDateTime dt) {
        String sql = "for $d in dataset taxiTrajectory; where $d.id = " + id + " and $d.at = " + dt + "; return $d";
        init(sql);
    }

    /*public TaxiTrajectoryAccessor(double dt, double southLan, double westLon, double widthLan, double heightLon) {
        String sql = "for $d in dataset taxiTrajectory; where $d.pos = " + id + " and $d.at = " + dt + "; return $d";
        init(sql);
    }*/

    @Override public List<JSONObject> get() {
        List<JSONObject> ret = new ArrayList<>();
        try {
            JSONArray resultList = new JSONArray(result);
            for (int i = 0; i < resultList.length(); i++) {
                ret.add(new JSONObject(resultList.get(i)));
            }
        } catch (JSONException e) {
            throw new ServiceException("Asterix DB trajectory return error." + e.getMessage());
        }
        return ret;
    }

    @Override public void execute() {
        conf.setBody(query);
        try {
            result = conn.handleRequest(conf, opType);
        } catch (Exception e) {
            throw new ServiceException("Asterix DB trajectory accessor error." + e.getMessage());
        }
    }

    @Override public void insert(List<JSONObject> record) {

    }
}
