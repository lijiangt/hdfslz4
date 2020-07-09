import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Lz4Codec;

/**
 * see: https://issues.apache.org/jira/browse/HADOOP-12990
 * @author lijt
 *
 */
public class HdfsLz4 {
	private static void usage() {
		System.out.println("Usage: ");
		System.out.println("java -jar hdfslz4.jar < inputfile > outputfile.lz4");
		System.out.println("java -jar hdfslz4.jar -d < inputfile.lz4 > outputfile");
		System.exit(1);
	}

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			compress(System.in, System.out);
		} else if ((args.length == 1 && "-d".equals(args[0].trim()))) {
			decompress(System.in, System.out);
		} else {
			usage();
		}
	}

	private static void compress(InputStream in, OutputStream out) throws IOException {
		Configuration conf = new Configuration();
		Lz4Codec codec = new Lz4Codec();
		codec.setConf(conf);
		try (CompressionOutputStream cos = codec.createOutputStream(out)) {
			IOUtils.copyBytes(in, cos, 4096, false);
			cos.finish();
		}
	}

	private static void decompress(InputStream in, OutputStream out) throws IOException {
		Configuration conf = new Configuration();
		Lz4Codec codec = new Lz4Codec();
		codec.setConf(conf);
		try (CompressionInputStream cis = codec.createInputStream(in)) {
			IOUtils.copyBytes(cis, out, 4096, false);
		} finally {
			out.close();
		}
	}
}
