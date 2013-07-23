package com.dp.blackhole.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import com.dp.blackhole.common.AppRegPB.AppReg;
import com.dp.blackhole.common.AppRollPB.AppRoll;
import com.dp.blackhole.common.MessagePB.Message;

public class Util {
	public static String readString(DataInputStream in) throws IOException {
		int length = in.readInt();
		byte[] data = new byte[length];
		in.readFully(data);
		return new String(data);
	}

	public static void writeString(String str ,DataOutputStream out) throws IOException {
		byte[] data = str.getBytes();
		out.writeInt(data.length);
		out.write(data);
	}
}
