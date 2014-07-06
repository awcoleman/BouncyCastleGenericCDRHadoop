package com.awcoleman.BouncyCastleGenericCDRHadoopWithWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;

import org.apache.hadoop.io.Writable;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Object;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.DERApplicationSpecific;

/**
 * 
 * A Writable container class for a "Simple Generic CDR".
 * 
 * @author awcoleman
 * @version 20140702
 * license: Apache License 2.0; http://www.apache.org/licenses/LICENSE-2.0
 * 
 */
public class CallDetailRecord extends ASN1Object implements Writable {

	ASN1Sequence cdr;

	int recordNumber;      //APPLICATION  2
	String callingNumber;  //APPLICATION  8
	String calledNumber;   //APPLICATION  9
	String startDate;      //APPLICATION 16
	String startTime;      //APPLICATION 18
	int duration;          //APPLICATION 19

	public CallDetailRecord() {
		clearFields();
	}

	public CallDetailRecord(ASN1Sequence inSeq) throws UnsupportedEncodingException {
		cdr = inSeq;
		
		for (@SuppressWarnings("unchecked")
		Enumeration<ASN1Encodable> en = cdr.getObjects(); en.hasMoreElements();) {
			ASN1Encodable em = en.nextElement();
			ASN1Primitive emp = em.toASN1Primitive();
			DERApplicationSpecific emt = (DERApplicationSpecific)emp;
			
			//System.out.println("emt.getApplicationTag(): "+emt.getApplicationTag());
			
			switch (emt.getApplicationTag()) {
				case 2: recordNumber = emt.getContents()[0];
					break;
				case 8: callingNumber = new String(emt.getContents(), "UTF-8");
					break;
				case 9: calledNumber = new String(emt.getContents(), "UTF-8");
					break;
				case 16: startDate = new String(emt.getContents(), "UTF-8");
					break;
				case 18: startTime = new String(emt.getContents(), "UTF-8");
					break;
				case 19: duration = emt.getContents()[0];
					break;
				default:
					//Unknown application number. In production would either log or error.
					break;
			}
		}
	
	}

	public void clearFields() {
		cdr=null;
		recordNumber=0;
		callingNumber=null;
		calledNumber=null;
		startDate=null;
		startTime=null;
		duration=0;
	}

	@Override
	public ASN1Primitive toASN1Primitive() {
		return null; //Once we have read in the original ASN.1 we are done with it.
	}

	public int getRecordNumber() {
		return recordNumber;
	}

	public void setRecordNumber(int recordNumber) {
		this.recordNumber = recordNumber;
	}

	public String getCallingNumber() {
		return callingNumber;
	}

	public void setCallingNumber(String callingNumber) {
		this.callingNumber = callingNumber;
	}

	public String getCalledNumber() {
		return calledNumber;
	}

	public void setCalledNumber(String calledNumber) {
		this.calledNumber = calledNumber;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		recordNumber = in.readInt();
		callingNumber = in.readUTF();
		calledNumber = in.readUTF();
		startDate = in.readUTF();
		startTime = in.readUTF();
		duration = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(recordNumber);
		out.writeUTF(callingNumber);  //writeUTF includes length info
		out.writeUTF(calledNumber);
		out.writeUTF(startDate);
		out.writeUTF(startTime);
		out.writeInt(duration);

	}

}
