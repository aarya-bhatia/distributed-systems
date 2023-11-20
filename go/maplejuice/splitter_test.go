package maplejuice

import "testing"
import "fmt"

func TestFileContents(t *testing.T) {
	fileContents := `This is the first line.
	This is the second line.
	And here goes the third line.`

	lines := ProcessFileContents(fileContents)

	for i, line := range lines {
		fmt.Printf("Line %d: Offset: %d, Length: %d\n", i+1, line.Offset, line.Length)
	}

	fmt.Println("Size of file:", len(fileContents))
}
