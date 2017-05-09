public class Finger {

	private Long hash;
	private int length;
	private long position;
	
	public Finger(Long hash, int length) {
		super();
		this.hash = hash;
		this.length = length;
	}
	
	public Finger(Long hash, int length, long position) {
		super();
		this.hash = hash;
		this.length = length;
		this.position = position;
	}
	
	@Override
	public int hashCode() {
		return hash.hashCode();//(int) (hash ^ (hash >>> 32));
	}
	
	@Override
	public boolean equals(Object obj) {
		Finger toCompare = (Finger) obj;
		return this.hash.hashCode() == toCompare.hash.hashCode();
	}

	/**
	 * @return the hash
	 */
	public Long getHash() {
		return hash;
	}
	/**
	 * @param hash the hash to set
	 */
	public void setHash(Long hash) {
		this.hash = hash;
	}
	/**
	 * @return the length
	 */
	public int getLength() {
		return length;
	}
	/**
	 * @param length the length to set
	 */
	public void setLength(int length) {
		this.length = length;
	}
	/**
	 * @return the position
	 */
	public long getPosition() {
		return position;
	}
	/**
	 * @param position the position to set
	 */
	public void setPosition(long position) {
		this.position = position;
	}
}
