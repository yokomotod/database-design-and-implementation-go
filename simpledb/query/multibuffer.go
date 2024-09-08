package query

// available以下で最も大きいsizeの約数を返す
// 例: (1000, 1000) => 1000, (500, 1000) => 500, (400, 1000) => 334
// 1000/1 = 1000, 1000/2 = 500, 1000/3 = 333.3...
func BufferNeedsBestFactor(available, size int32) int32 {
	avail := available - 2
	if avail <= 1 {
		return 1
	}

	k := size
	i := 1.0
	for k > avail {
		i++
		k = int32(float64(size) / i)
	}

	return k
}
