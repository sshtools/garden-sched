package com.sshtools.gardensched;

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterID implements Streamable {
	private Address owner;
	private int id;
	private String strId;

	private final static AtomicInteger nextId = new AtomicInteger(1);

	public ClusterID() {
	}

	public ClusterID(Address owner, int id) {
		this.owner = owner;
		this.id = id;
	}

	public ClusterID(String strId) {
		this.strId = strId;
	}

	public int getId() {
		return strId == null ? id : strId.hashCode();
	}

	public String getStrId() {
		return strId;
	}

	public static synchronized ClusterID createNext(String strId) {
		return new ClusterID(strId);
	}

	public static synchronized ClusterID createNext(Address addr) {
		return new ClusterID(addr, nextId.getAndAdd(1));
	}

	public int hashCode() {
		return strId == null ? owner.hashCode() + id : strId.hashCode();
	}

	public boolean equals(Object obj) {
		ClusterID other = (ClusterID) obj;
		if (strId == null)
			return owner.equals(other.owner) && id == other.id;
		else
			return strId.equals(other.strId);
	}

	public String toString() {
		return strId == null ? owner + "::" + id : strId;
	}

	public void writeTo(DataOutput out) throws IOException {
		Util.writeAddress(owner, out);
		out.writeInt(id);
		out.writeUTF(strId);
	}

	public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
		owner = Util.readAddress(in);
		id = in.readInt();
		strId = in.readUTF();
	}

	public static ClusterID parse(String cid) {
		var idx = cid.indexOf("::");
		if(idx > -1) {
			/* TODO hrm. this is not great, I think ClusterID will have to go as the main job identifier and replace
			 * it with a string
			 */
			try {
				return new ClusterID(new IpAddress(cid.substring(0, cid.indexOf(".."))), Integer.parseInt(cid.substring(idx + 2)));
			} catch (Exception e) {
				throw new IllegalArgumentException(e);
			}
		}
		else  {
			return new ClusterID(cid);
		}
	}
}
