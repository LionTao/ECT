package cn.edu.suda.ada.ect;

import cn.edu.suda.ada.ect.pushdown.SpatialQuery;
import com.uber.h3core.H3Core;
import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class BitMapBench {
    @Data
    @Builder
    private static class Result {
        int QueryNo;
        long Card;
        long time;
        String Name;

    }
    public static void main(String[] args) throws IOException {
        List<Result> csvResult = new LinkedList<>();
        var h3 = H3Core.newInstance();
        var rr = new Roaring64NavigableMap();
        try(FileReader reader = new FileReader("/var/home/liontao/work/stdtw/all_table.csv")){
            BufferedReader br = new BufferedReader(reader);
            String line;
            int c=0;
            br.readLine();
            while((line = br.readLine()) != null) {
                // 一行一行地处理...
                c+=1;
                var data = line.split(",");
                rr.add(h3.latLngToCell(Double.parseDouble(data[3]),Double.parseDouble(data[2]),15));
                System.out.println(rr.getLongCardinality());
                if (c%10==0){
                    csvResult.add(Result.builder()
                                    .Name("bitmap")
                                    .QueryNo(c)
                                    .Card(rr.getLongCardinality()*40)
                            .build());
                }
                if (c>200){
                    break;
                }
            }
            writeCSV(csvResult,"bitmap.csv");


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeCSV(List<Result> csvResult, String fname) throws IOException {
        StringWriter sw = new StringWriter();
        String[] HEADERS = {"name", "number", "time", "traffic"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();
        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
            csvResult.forEach((c) -> {
                try {
                    printer.printRecord(c.getName(), c.getQueryNo(), c.getTime(), c.getCard());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write(sw.toString());

        writer.close();
    }
}
