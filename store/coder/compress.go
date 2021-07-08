package coder

import "fmt"

// 针对byte形数据优化的simeple8b算法
// ┌──────────────┬─────────────────────────────────────────────────────────────┐
// │   Selector   │       0    1   2   3   4   5   6   7  8  9 10 11 12 13 14 15│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │     Bits     │       0(0) 0(1)0(0)0(1)0(0)0(1)1   1  2  2  3  4  5  7  8  8│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │      N       │     240  240 120 120  60  60  28  22 14 12  9  7  5  4  3  1│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │   Wasted Bits│      28   28  28  28  28  28   0   6  0  4  1  0  3  0  4 20│
// └──────────────┴─────────────────────────────────────────────────────────────┘

func Decompress(dst *[240]byte, v uint32) (n int, err error) {
	sel := v >> 28
	if sel >= 16 {
		return 0, fmt.Errorf("invalid selector value: %b", sel)
	}
	selector[sel].unpack(v, dst)
	return selector[sel].n, nil
}

type packing struct {
	n, bit int
	unpack func(uint32, *[240]byte)
}

var selector [16]packing = [16]packing{
	{240, 0, unpack_0},
	{240, 0, unpack_1},
	{120, 0, unpack_0},
	{120, 0, unpack_1},
	{60, 0, unpack_0},
	{60, 0, unpack_1},
	{28, 1, unpack_bit1},
	{22, 1, unpack_bit1},
	{14, 2, unpack_bit2},
	{12, 2, unpack_bit2},
	{9, 3, unpack_bit3},
	{7, 4, unpack_bit4},
	{5, 5, unpack_bit5},
	{4, 7, unpack_bit7},
	{3, 8, unpack_bit8},
	{1, 8, unpack_bit8},
}

func CompressAll(src []byte) ([]uint32, error) {
	i := 0

	// Re-use the input slice and write encoded values back in place
	dst := make([]uint32, len(src))
	j := 0

	for {
		if i >= len(src) {
			break
		}
		remaining := src[i:]

		if canPack(remaining, 240, 0, 0) {
			//case 0
			dst[j] = 0
			i += 240
		} else if canPack(remaining, 240, 0, 1) {
			//case 1
			dst[j] = 1 << 28
			i += 240
		} else if canPack(remaining, 120, 0, 0) {
			//case 2
			dst[j] = 2 << 28
			i += 120
		} else if canPack(remaining, 120, 0, 1) {
			//case 3
			dst[j] = 3 << 28
			i += 120
		} else if canPack(remaining, 60, 0, 0) {
			//case 4
			dst[j] = 4 << 28
			i += 60
		} else if canPack(remaining, 60, 0, 1) {
			//case 5
			dst[j] = 5 << 28
			i += 60
		} else if canPack(remaining, 28, 1, 0) {
			//case 6
			dst[j] = pack28(src[i : i+28])
			i += 28
		} else if canPack(remaining, 22, 1, 0) {
			//case 7
			dst[j] = pack22(src[i : i+22])
			i += 22
		} else if canPack(remaining, 14, 2, 0) {
			//case 8
			dst[j] = pack14(src[i : i+14])
			i += 14
		} else if canPack(remaining, 12, 2, 0) {
			//case 9
			dst[j] = pack12(src[i : i+12])
			i += 12
		} else if canPack(remaining, 9, 3, 0) {
			//case 10
			dst[j] = pack9(src[i : i+9])
			i += 9
		} else if canPack(remaining, 7, 4, 0) {
			//case 11
			dst[j] = pack7(src[i : i+7])
			i += 7
		} else if canPack(remaining, 5, 5, 0) {
			//case 12
			dst[j] = pack5(src[i : i+5])
			i += 5
		} else if canPack(remaining, 4, 7, 0) {
			//case 13
			dst[j] = pack4(src[i : i+4])
			i += 4
		} else if canPack(remaining, 3, 8, 0) {
			//case 14
			dst[j] = pack3(src[i : i+3])
			i += 3
		} else if canPack(remaining, 1, 8, 0) {
			//case 15
			dst[j] = pack1(src[i : i+1])
			i += 1
		} else {
			return nil, fmt.Errorf("value out of bounds")
		}
		j += 1
	}
	return dst[:j], nil
}

// canPack returs true if n elements from in can be stored using bits per element
func canPack(src []byte, n, bits int, expect byte) bool {
	if len(src) < n {
		return false
	}

	end := len(src)
	if n < end {
		end = n
	}

	// Selector 0,1 are special and use 0 bits to encode runs of 1's
	if bits == 0 {
		for i, v := range src {
			if i >= end {
				return true
			}
			if v != expect {
				return false
			}
		}
		return true
	}

	max := uint32((1 << uint32(bits)) - 1)

	for i := 0; i < end; i++ {
		if uint32(src[i]) > max {
			return false
		}
	}

	return true
}

// pack28 packs 28 values from in using 1 bits each
func pack28(src []byte) uint32 {
	return 6<<28 |
		uint32(src[0]) |
		uint32(src[1])<<1 |
		uint32(src[2])<<2 |
		uint32(src[3])<<3 |
		uint32(src[4])<<4 |
		uint32(src[5])<<5 |
		uint32(src[6])<<6 |
		uint32(src[7])<<7 |
		uint32(src[8])<<8 |
		uint32(src[9])<<9 |
		uint32(src[10])<<10 |
		uint32(src[11])<<11 |
		uint32(src[12])<<12 |
		uint32(src[13])<<13 |
		uint32(src[14])<<14 |
		uint32(src[15])<<15 |
		uint32(src[16])<<16 |
		uint32(src[17])<<17 |
		uint32(src[18])<<18 |
		uint32(src[19])<<19 |
		uint32(src[20])<<20 |
		uint32(src[21])<<21 |
		uint32(src[22])<<22 |
		uint32(src[23])<<23 |
		uint32(src[24])<<24 |
		uint32(src[25])<<25 |
		uint32(src[26])<<26 |
		uint32(src[27])<<27
}

// pack22 packs 22 values from in using 1 bits each
func pack22(src []byte) uint32 {
	return 7<<28 |
		uint32(src[0]) |
		uint32(src[1])<<1 |
		uint32(src[2])<<2 |
		uint32(src[3])<<3 |
		uint32(src[4])<<4 |
		uint32(src[5])<<5 |
		uint32(src[6])<<6 |
		uint32(src[7])<<7 |
		uint32(src[8])<<8 |
		uint32(src[9])<<9 |
		uint32(src[10])<<10 |
		uint32(src[11])<<11 |
		uint32(src[12])<<12 |
		uint32(src[13])<<13 |
		uint32(src[14])<<14 |
		uint32(src[15])<<15 |
		uint32(src[16])<<16 |
		uint32(src[17])<<17 |
		uint32(src[18])<<18 |
		uint32(src[19])<<19 |
		uint32(src[20])<<20 |
		uint32(src[21])<<21
}

// pack14 packs 14 values from in using 2 bits each
func pack14(src []byte) uint32 {
	return 8<<28 |
		uint32(src[0]) |
		uint32(src[1])<<2 |
		uint32(src[2])<<4 |
		uint32(src[3])<<6 |
		uint32(src[4])<<8 |
		uint32(src[5])<<10 |
		uint32(src[6])<<12 |
		uint32(src[7])<<14 |
		uint32(src[8])<<16 |
		uint32(src[9])<<18 |
		uint32(src[10])<<20 |
		uint32(src[11])<<22 |
		uint32(src[12])<<24 |
		uint32(src[13])<<26
}

// pack12 packs 12 values from in using 2 bits each
func pack12(src []byte) uint32 {
	return 9<<28 |
		uint32(src[0]) |
		uint32(src[1])<<2 |
		uint32(src[2])<<4 |
		uint32(src[3])<<6 |
		uint32(src[4])<<8 |
		uint32(src[5])<<10 |
		uint32(src[6])<<12 |
		uint32(src[7])<<14 |
		uint32(src[8])<<16 |
		uint32(src[9])<<18 |
		uint32(src[10])<<20 |
		uint32(src[11])<<22
}

// pack9 packs 9 values from in using 3 bits each
func pack9(src []byte) uint32 {
	return 10<<28 |
		uint32(src[0]) |
		uint32(src[1])<<3 |
		uint32(src[2])<<6 |
		uint32(src[3])<<9 |
		uint32(src[4])<<12 |
		uint32(src[5])<<15 |
		uint32(src[6])<<18 |
		uint32(src[7])<<21 |
		uint32(src[8])<<24
}

// pack7 packs 7 values from in using 4 bits each
func pack7(src []byte) uint32 {
	return 11<<28 |
		uint32(src[0]) |
		uint32(src[1])<<4 |
		uint32(src[2])<<8 |
		uint32(src[3])<<12 |
		uint32(src[4])<<16 |
		uint32(src[5])<<20 |
		uint32(src[6])<<24
}

// pack5 packs 5 values from in using 5 bits each
func pack5(src []byte) uint32 {
	return 12<<28 |
		uint32(src[0]) |
		uint32(src[1])<<5 |
		uint32(src[2])<<10 |
		uint32(src[3])<<15 |
		uint32(src[4])<<20
}

// pack4 packs 4 values from in using 7 bits each
func pack4(src []byte) uint32 {
	return 13<<28 |
		uint32(src[0]) |
		uint32(src[1])<<7 |
		uint32(src[2])<<14 |
		uint32(src[3])<<21
}

// pack3 packs 3 values from in using 8 bits each
func pack3(src []byte) uint32 {
	return 14<<28 |
		uint32(src[0]) |
		uint32(src[1])<<8 |
		uint32(src[2])<<16
}

// pack1 packs 1 values from in using 8 bits each
func pack1(src []byte) uint32 {
	return 15<<28 |
		uint32(src[0])
}

func unpack_0(v uint32, dst *[240]byte) {
	for i := range dst {
		dst[i] = 0
	}
}

func unpack_1(v uint32, dst *[240]byte) {
	for i := range dst {
		dst[i] = 1
	}
}

func unpack_bit1(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 1)
	dst[1] = byte((v >> 1) & 1)
	dst[2] = byte((v >> 2) & 1)
	dst[3] = byte((v >> 3) & 1)
	dst[4] = byte((v >> 4) & 1)
	dst[5] = byte((v >> 5) & 1)
	dst[6] = byte((v >> 6) & 1)
	dst[7] = byte((v >> 7) & 1)
	dst[8] = byte((v >> 8) & 1)
	dst[9] = byte((v >> 9) & 1)
	dst[10] = byte((v >> 10) & 1)
	dst[11] = byte((v >> 11) & 1)
	dst[12] = byte((v >> 12) & 1)
	dst[13] = byte((v >> 13) & 1)
	dst[14] = byte((v >> 14) & 1)
	dst[15] = byte((v >> 15) & 1)
	dst[16] = byte((v >> 16) & 1)
	dst[17] = byte((v >> 17) & 1)
	dst[18] = byte((v >> 18) & 1)
	dst[19] = byte((v >> 19) & 1)
	dst[20] = byte((v >> 20) & 1)
	dst[21] = byte((v >> 21) & 1)
	dst[22] = byte((v >> 22) & 1)
	dst[23] = byte((v >> 23) & 1)
	dst[24] = byte((v >> 24) & 1)
	dst[25] = byte((v >> 25) & 1)
	dst[26] = byte((v >> 26) & 1)
	dst[27] = byte((v >> 27) & 1)
}

func unpack_bit2(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 3)
	dst[1] = byte((v >> 2) & 3)
	dst[2] = byte((v >> 4) & 3)
	dst[3] = byte((v >> 6) & 3)
	dst[4] = byte((v >> 8) & 3)
	dst[5] = byte((v >> 10) & 3)
	dst[6] = byte((v >> 12) & 3)
	dst[7] = byte((v >> 14) & 3)
	dst[8] = byte((v >> 16) & 3)
	dst[9] = byte((v >> 18) & 3)
	dst[10] = byte((v >> 20) & 3)
	dst[11] = byte((v >> 22) & 3)
	dst[12] = byte((v >> 24) & 3)
	dst[13] = byte((v >> 26) & 3)
}

func unpack_bit3(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 7)
	dst[1] = byte((v >> 3) & 7)
	dst[2] = byte((v >> 6) & 7)
	dst[3] = byte((v >> 9) & 7)
	dst[4] = byte((v >> 12) & 7)
	dst[5] = byte((v >> 15) & 7)
	dst[6] = byte((v >> 18) & 7)
	dst[7] = byte((v >> 21) & 7)
	dst[8] = byte((v >> 24) & 7)
}

func unpack_bit4(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 15)
	dst[1] = byte((v >> 4) & 15)
	dst[2] = byte((v >> 8) & 15)
	dst[3] = byte((v >> 12) & 15)
	dst[4] = byte((v >> 16) & 15)
	dst[5] = byte((v >> 20) & 15)
	dst[6] = byte((v >> 24) & 15)
}

func unpack_bit5(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 31)
	dst[1] = byte((v >> 5) & 31)
	dst[2] = byte((v >> 10) & 31)
	dst[3] = byte((v >> 15) & 31)
	dst[4] = byte((v >> 20) & 31)
}

func unpack_bit7(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 127)
	dst[1] = byte((v >> 7) & 127)
	dst[2] = byte((v >> 14) & 127)
	dst[3] = byte((v >> 21) & 127)
}

func unpack_bit8(v uint32, dst *[240]byte) {
	dst[0] = byte(v & 255)
	dst[1] = byte((v >> 8) & 255)
	dst[2] = byte((v >> 16) & 255)
}
