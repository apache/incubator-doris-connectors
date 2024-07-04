package org.apache.doris.common.io;

import org.roaringbitmap.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copied from Apache Doris
 */
public class BitmapValue {

    public static final int EMPTY = 0;
    public static final int SINGLE32 = 1;
    public static final int BITMAP32 = 2;
    public static final int SINGLE64 = 3;
    public static final int BITMAP64 = 4;

    public static final int SINGLE_VALUE = 1;
    public static final int BITMAP_VALUE = 2;

    public static final long UNSIGNED_32BIT_INT_MAX_VALUE = 4294967295L;

    private int bitmapType;
    private long singleValue;
    private Roaring64Map bitmap;

    public BitmapValue() {
        bitmapType = EMPTY;
    }

    public void add(int value) {
        add(Util.toUnsignedLong(value));
    }

    public void add(long value) {
        switch (bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                singleValue = value;
                bitmapType = SINGLE_VALUE;
                break;
            case SINGLE_VALUE:
                if (this.singleValue != value) {
                    bitmap = new Roaring64Map();
                    bitmap.add(value);
                    bitmap.add(singleValue);
                    bitmapType = BITMAP_VALUE;
                }
                break;
            case BITMAP_VALUE:
                bitmap.addLong(value);
                break;
        }
    }

    public boolean contains(int value) {
        return contains(Util.toUnsignedLong(value));
    }

    public boolean contains(long value) {
        switch (bitmapType) {
            case EMPTY:
                return false;
            case SINGLE_VALUE:
                return singleValue == value;
            case BITMAP_VALUE:
                return bitmap.contains(value);
            default:
                return false;
        }
    }

    public long cardinality() {
        switch (bitmapType) {  // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                return 0;
            case SINGLE_VALUE:
                return 1;
            case BITMAP_VALUE:
                return bitmap.getLongCardinality();
        }
        return 0;
    }

    public void serialize(DataOutput output) throws IOException {
        switch (bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                output.writeByte(EMPTY);
                break;
            case SINGLE_VALUE:
                // is 32-bit enough
                // FE is big end but BE is little end.
                if (isLongValue32bitEnough(singleValue)) {
                    output.write(SINGLE32);
                    output.writeInt(Integer.reverseBytes((int) singleValue));
                } else {
                    output.writeByte(SINGLE64);
                    output.writeLong(Long.reverseBytes(singleValue));
                }
                break;
            case BITMAP_VALUE:
                bitmap.serialize(output);
                break;
        }
    }

    public void deserialize(DataInput input) throws IOException {
        clear();
        int bitmapType = input.readByte();
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE32:
                singleValue = Util.toUnsignedLong(Integer.reverseBytes(input.readInt()));
                this.bitmapType = SINGLE_VALUE;
                break;
            case SINGLE64:
                singleValue = Long.reverseBytes(input.readLong());
                this.bitmapType = SINGLE_VALUE;
                break;
            case BITMAP32:
            case BITMAP64:
                bitmap = bitmap == null ? new Roaring64Map() : bitmap;
                bitmap.deserialize(input, bitmapType);
                this.bitmapType = BITMAP_VALUE;
                break;
            default:
                throw new RuntimeException(String.format("unknown bitmap type %s ", bitmapType));
        }
    }

    // In-place bitwise AND (intersection) operation. The current bitmap is modified.
    public void and(BitmapValue other) {
        switch (other.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                clear();
                break;
            case SINGLE_VALUE:
                switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (this.singleValue != other.singleValue) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        if (!this.bitmap.contains(other.singleValue)) {
                            clear();
                        } else {
                            clear();
                            this.singleValue = other.singleValue;
                            this.bitmapType = SINGLE_VALUE;
                        }
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (!other.bitmap.contains(this.singleValue)) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.and(other.bitmap);
                        convertToSmallerType();
                        break;
                }
                break;
        }
    }

    // In-place bitwise OR (union) operation. The current bitmap is modified.
    public void or(BitmapValue other) {
        switch (other.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                break;
            case SINGLE_VALUE:
                add(other.singleValue);
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case EMPTY:
                        // deep copy the bitmap in case of multi-rollups update the bitmap repeatedly
                        this.bitmap = new Roaring64Map();
                        this.bitmap.or(other.bitmap);
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case SINGLE_VALUE:
                        this.bitmap = new Roaring64Map();
                        this.bitmap.or(other.bitmap);
                        this.bitmap.add(this.singleValue);
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.or(other.bitmap);
                        break;
                }
                break;
        }
    }

    public void remove(long value) {
        switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                break;
            case SINGLE_VALUE:
                if (this.singleValue == value) {
                    clear();
                }
                break;
            case BITMAP_VALUE:
                this.bitmap.removeLong(value);
                convertToSmallerType();
                break;
        }
    }

    // In-place bitwise ANDNOT (difference) operation. The current bitmap is modified
    public void not(BitmapValue other) {
        switch (other.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                break;
            case SINGLE_VALUE:
                remove(other.singleValue);
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (other.bitmap.contains(this.singleValue)) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.andNot(other.bitmap);
                        convertToSmallerType();
                        break;
                }
                break;
        }
    }

    // In-place bitwise XOR (symmetric difference) operation. The current bitmap is modified
    public void xor(BitmapValue other) {
        switch (other.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                break;
            case SINGLE_VALUE:
                switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case EMPTY:
                        add(other.singleValue);
                        break;
                    case SINGLE_VALUE:
                        if (this.singleValue != other.singleValue) {
                            add(other.singleValue);
                        } else {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        if (!this.bitmap.contains(other.singleValue)) {
                            this.bitmap.add(other.singleValue);
                        } else {
                            this.bitmap.removeLong(other.singleValue);
                            convertToSmallerType();
                        }
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case EMPTY:
                        this.bitmap = other.bitmap;
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case SINGLE_VALUE:
                        this.bitmap = other.bitmap;
                        this.bitmapType = BITMAP_VALUE;
                        if (this.bitmap.contains(this.singleValue)) {
                            this.bitmap.removeLong(this.singleValue);
                        } else {
                            this.bitmap.add(this.bitmapType);
                        }
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.xor(other.bitmap);
                        convertToSmallerType();
                        break;
                }
                break;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof BitmapValue)) {
            return false;
        }
        boolean ret = false;
        if (this.bitmapType != ((BitmapValue) other).bitmapType) {
            return false;
        }
        switch (((BitmapValue) other).bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                ret = true;
                break;
            case SINGLE_VALUE:
                ret = this.singleValue == ((BitmapValue) other).singleValue;
                break;
            case BITMAP_VALUE:
                ret = bitmap.equals(((BitmapValue) other).bitmap);
        }
        return ret;
    }

    /**
     *  usage note:
     *      now getSizeInBytes is different from be' impl
     *      The reason is that java's roaring didn't implement method #shrinkToFit but be's getSizeInBytes need it
     *      Implementing java's shrinkToFit means refactor roaring whose fields are all unaccess in Doris Fe's package
     *      That would be an another big project
     */
    // TODO(wb): keep getSizeInBytes consistent with be and refactor roaring
    public long getSizeInBytes() {
        long size = 0;
        switch (bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                size = 1;
                break;
            case SINGLE_VALUE:
                if (isLongValue32bitEnough(singleValue)) {
                    size = 1 + 4;
                } else {
                    size = 1 + 8;
                }
                break;
            case BITMAP_VALUE:
                size = 1 + bitmap.getSizeInBytes();
        }
        return size;
    }

    @Override
    public String toString() {
        String toStringStr = "{}";
        switch (bitmapType) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case EMPTY:
                break;
            case SINGLE_VALUE:
                toStringStr = String.format("{%s}", singleValue);
                break;
            case BITMAP_VALUE:
                toStringStr = this.bitmap.toString();
                break;
        }
        return toStringStr;
    }

    public void clear() {
        this.bitmapType = EMPTY;
        this.singleValue = -1;
        this.bitmap = null;
    }

    private void convertToSmallerType() {
        if (bitmapType == BITMAP_VALUE) {
            if (bitmap.getLongCardinality() == 0) {
                this.bitmap = null;
                this.bitmapType = EMPTY;
            } else if (bitmap.getLongCardinality() == 1) {
                this.singleValue = bitmap.select(0);
                this.bitmapType = SINGLE_VALUE;
                this.bitmap = null;
            }
        }
    }

    private boolean isLongValue32bitEnough(long value) {
        return value <= UNSIGNED_32BIT_INT_MAX_VALUE;
    }

    // just for ut
    public int getBitmapType() {
        return bitmapType;
    }

    // just for ut
    public boolean is32BitsEnough() {
        switch (bitmapType) {
            case EMPTY:
                return true;
            case SINGLE_VALUE:
                return isLongValue32bitEnough(singleValue);
            case BITMAP_VALUE:
                return bitmap.is32BitsEnough();
            default:
                return false;
        }
    }
}