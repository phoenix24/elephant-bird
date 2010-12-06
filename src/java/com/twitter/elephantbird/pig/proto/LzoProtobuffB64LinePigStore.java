package com.twitter.elephantbird.pig.proto;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;

/**
 * 
 * Simple store function that uses the LzoProtobufB64LineOutputFormat to write
 * Protobuf base64 lzo line output.<br/>
 * 
 * The class has a required string parameter. <br/>
 * This is used as a friendly property mapping to ProtoBuff class and abstracts
 * the pig scripts from having to contain the actual protobuf class names.
 * <p/>
 * e.g.<br/>
 * <code>
 * a = LOAD '$INPUT' using com.twitter.elephantbird.pig.store.LzoProtobuffB64LineStore('person');
 * </code> <br/>
 * The above code will look for a properties declared person.<br/>
 * If we have this in the script itself or better in the
 * $PIG_HOME/conf/pig.properties like so:<br/>
 * person=MyProtoClass<br/>
 * Then the this loader will get the MyProtoClass from the configuration and use
 * it to write all Tuples.
 * 
 * 
 * 
 * 
 * 
 */
public class LzoProtobuffB64LinePigStore extends PigStorage implements
		LoadMetadata {

	String clsMapping;

	private Function<byte[], ? extends Message> protoConverter;
	private final Base64 base64 = new Base64();
	private final ProtobufToPig protoToPig = new ProtobufToPig();

	private int[] requiredIndices = null;

	private boolean requiredIndicesInitialized = false;

	PigCounterHelper counterHelper = new PigCounterHelper();

	private String signature;

	protected enum LzoProtobuffB64LinePigStoreCounts {
		LinesRead, ProtobufsRead
	}

	public LzoProtobuffB64LinePigStore(String clsMapping) {
		this.clsMapping = clsMapping;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OutputFormat getOutputFormat() {
		return new LzoProtobufB64LineOutputFormat(clsMapping);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new LzoTextInputFormat();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		super.setStoreLocation(location, job);

		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		FileOutputFormat.setOutputPath(job, new Path(location));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	}

	/**
	 * Creates the requiredIndices int array from the UDFContext.<br/>
	 * This action is only performed once, subsequent method calls will only
	 * return.
	 * 
	 * @throws IOException
	 */
	private void checkRequiredColumnsInit() throws IOException {
		if (!requiredIndicesInitialized) {
			requiredIndicesInitialized = true;
			String value = UDFContext.getUDFContext()
					.getUDFProperties(this.getClass()).getProperty(signature);

			if (value != null) {
				this.requiredIndices = (int[]) ObjectSerializer
						.deserialize(value);
			}

		}
	}

	@Override
	public Tuple getNext() throws IOException {

		try {
			// check that the required columns indices have been read if any
			checkRequiredColumnsInit();
			
			boolean notDone = in.nextKeyValue();
			if (!notDone) {
				return null;
			}

			// READ the ProtoBuff Value (String => Decode => Parse => Message =>
			// Tuple)
			Text value = (Text) in.getCurrentValue();

			// incrCounter(LzoProtobuffB64LinePigStoreCounts.LinesRead, 1L);

			Message protoValue = protoConverter.apply(base64.decode(value
					.toString().getBytes("UTF-8")));

			if (protoValue == null) {
				throw new RuntimeException("Error converting line to protobuff");
			}

			return new ProtobufTuple(protoValue, requiredIndices);

		} catch (Exception e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

	}

	protected void incrCounter(Enum<?> key, long incr) {
		counterHelper.incrCounter(key, incr);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		super.prepareToRead(reader, split);

		protoConverter = Protobufs.getProtoConverter(ProtobufClassUtil
				.loadProtoClass(clsMapping, split.getConf()));
	
	}

	@Override
	public ResourceSchema getSchema(String filename, Job job)
			throws IOException {
		return new ResourceSchema(protoToPig.toSchema(Protobufs
				.getMessageDescriptor(ProtobufClassUtil.loadProtoClass(
						clsMapping, job.getConfiguration()))));

	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		RequiredFieldResponse response = null;

		if (!(requiredFieldList == null || requiredFieldList.getFields() == null)) {

			// convert the list of RequiredFieldList objects into an array of
			// int
			// each item in the array containns the required field index.
			// Note that this array of int is sorted.
			List<RequiredField> requiredFields = requiredFieldList.getFields();

			int requiredIndices[] = new int[requiredFields.size()];

			for (int i = 0; i < requiredFields.size(); i++) {
				requiredIndices[i] = requiredFields.get(i).getIndex();
			}

			// we must sort this array. The logic that reads from it required
			// this.
			// this is a map between the required Index and the real index
			// e.g. [0] => maps to [3]
			Arrays.sort(requiredIndices);

			try {
				UDFContext
						.getUDFContext()
						.getUDFProperties(this.getClass())
						.setProperty(signature,
								ObjectSerializer.serialize(requiredIndices));
			} catch (Exception e) {
				throw new RuntimeException("Cannot serialize requiredIndices");
			}

			response = new RequiredFieldResponse(true);
		}

		return response;
	}

	@Override
	public void setUDFContextSignature(String signature) {
		this.signature = signature;
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
	}

}
