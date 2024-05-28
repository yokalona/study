package com.yokalona.array.persitent.serializers;

public final class Version implements Comparable<Version> {

    public static final TypeDescriptor<Version> descriptor = new TypeDescriptor<>(4, Version.class);
    static {
        SerializerStorage.register(descriptor, new VersionSerializer(descriptor));
    }

    private final byte critical;
    private final byte major;
    private final byte minor;

    private byte mode;

    public Version(byte critical, byte major, byte minor, byte mode) {
        this.critical = critical;
        this.major = major;
        this.minor = minor;
        this.mode = mode;
    }

    public Version(int critical, int major, int minor, int mode) {
        this((byte) critical, (byte) major, (byte) minor, (byte) mode);
    }

    public Version
    copy() {
        return new Version(critical, major, minor, mode);
    }

    @Override
    public int
    compareTo(Version other) {
        int critical = Byte.compare(this.critical, other.critical);
        if (critical != 0) return critical;
        else return Byte.compare(this.major, other.major);
    }

    @Override
    public String
    toString() {
        return String.format("%d.%d.%d mode=%d", critical, major, minor, mode);
    }

    public byte
    mode() {
        return mode;
    }

    public void
    mode(byte mode) {
        this.mode = mode;
    }

    public record VersionSerializer(TypeDescriptor<Version> descriptor, byte[] bytes) implements Serializer<Version> {

        public VersionSerializer(TypeDescriptor<Version> descriptor) {
            this(descriptor, new byte[descriptor.size()]);
        }

        @Override
        public byte[]
        serialize(Version version) {
            return new byte[]{version.critical, version.major, version.minor, version.mode};
        }

        @Override
        public Version
        deserialize(byte[] bytes) {
            return deserialize(bytes, 0);
        }

        @Override
        public Version
        deserialize(byte[] bytes, int offset) {
            return new Version(bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]);
        }
    }
}
