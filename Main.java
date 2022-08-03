import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;


public class Main {
    public static void main(String[] args) throws IOException, ParseException {
        String ConnectionString = "mongodb://192.168.57.102:27017";
        String path = "/home/rk_data/harvesters/osm/";
        String regionFile = "";
        if (args[0] != null) {
            String database = args[0];

            MongoClient mongo = MongoClients.create(ConnectionString);
            MongoDatabase db = mongo.getDatabase(database);

            MongoDatabase metadata = mongo.getDatabase("rk_metadata");
            MongoCollection<Document> regionState = metadata.getCollection("regionState");
            MongoCursor<Document> rS = regionState.find().iterator();
            while(rS.hasNext()) {
                Document d = rS.next();
                String name = (String) d.get("database");
                if(name.equals(database)) {
                    ArrayList sources = (ArrayList) d.get("sources");
                    for(int i = 0; i < sources.size(); i++) {
                        Document obj = (Document) sources.get(i);
                        String type = (String) obj.get("type");
                        if(type.equals("osm")) {
                            Document params = (Document) obj.get("params");
                            regionFile = (String) params.get("regionFile");
                        }
                    }
                }
            }
            String[] split = regionFile.split("/");
                for (int i = 0; i < split.length-1; i++) {
                    path += split[i];
                    path += "/";
                }
            path += "highway-line.geojson";

            MongoCollection<Document> roads = db.getCollection("ta_roads");
            if(roads.countDocuments() == 0) {
                JSONParser jsonParser = new JSONParser();
                Reader reader = new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8);
                JSONObject obj = (JSONObject) jsonParser.parse(reader);
                JSONArray features = (JSONArray) obj.get("features");
                for (int i = 0; i < features.size(); i++) {
                    Document res = new Document();
                    JSONObject d = (JSONObject) features.get(i);
                    JSONObject properties = (JSONObject) d.get("properties");
                    JSONObject geo = (JSONObject) d.get("geometry");
                    res.put("NAME", properties.get("NAME"));
                    res.put("HIGHWAY", properties.get("HIGHWAY"));
                    res.put("Geometry", geo);
                    roads.insertOne(res);
                }

                roads.createIndex(Indexes.ascending("HIGHWAY"));
                roads.createIndex(Indexes.geo2dsphere("Geometry"));
            }
            MongoCollection<Document> houses = db.getCollection("mar_houses");
            MongoCursor<Document> hc = houses.find().iterator();
            AggregateIterable aggregation = null;

            while (hc.hasNext()) {

               Document d = hc.next();
                ArrayList geo = (ArrayList) d.get("centroid");
                if (geo != null) {
                    ObjectId id = (ObjectId) d.get("_id");
                    aggregation = roads.aggregate(Arrays.asList(new Document("$geoNear",
                                    new Document("near",
                                            new Document("type", "Point")
                                                    .append("coordinates", Arrays.asList(geo.get(0), geo.get(1))))
                                            .append("distanceField", "dist")
                                            .append("maxDistance", 500L)
                                            .append("spherical", true)),
                            new Document("$project",
                                    new Document("HIGHWAY", 1L)
                                            .append("roadWeight",
                                                    new Document("$switch",
                                                            new Document("branches", Arrays.asList(new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "primary")))
                                                                            .append("then", 4L),
                                                                    new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "primary_link")))
                                                                            .append("then", 4L),
                                                                    new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "secondary")))
                                                                            .append("then", 3L),
                                                                    new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "secondary_link")))
                                                                            .append("then", 3L),
                                                                    new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "tertiary")))
                                                                            .append("then", 2L),
                                                                    new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "tertiary_link")))
                                                                            .append("then", 2L),
                                                                    new Document("case",
                                                                            new Document("$eq", Arrays.asList("$HIGHWAY", "residential")))
                                                                            .append("then", 1L)))
                                                                    .append("default", 0L)))),
                            new Document("$group",
                                    new Document("_id", id)
                                            .append("weight",
                                                    new Document("$sum", "$roadWeight")))));


                    long weight;
                    MongoCursor<Document> rr = aggregation.iterator();
                    while (rr.hasNext()) {
                        Document dc = rr.next();
                        weight = (long) dc.get("weight");
                        BasicDBObject obj = new BasicDBObject();
                        obj.put("_id", id);
                        JSONObject params = new JSONObject();
                        params.put("AutoAccessibility", weight);
                        BasicDBObject updateObj = new BasicDBObject();
                        updateObj.put("$set", params);
                        houses.updateOne(obj, updateObj);
                    }
                }
            }
        }
    }
}

