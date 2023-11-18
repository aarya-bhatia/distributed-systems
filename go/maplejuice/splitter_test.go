package maplejuice

import "testing"
import "fmt"

func TestFileContents(t *testing.T) {
	fileContents := `This is the first line.
	This is the second line.
	And here goes the third line.`

	lines := ProcessFileContents(fileContents)

	// Display lines, their data, offset, and length information
	for i, line := range lines {
		fmt.Printf("Line %d: %s\n", i+1, line.Data)
		fmt.Printf("   Offset: %d, Length: %d\n", line.Offset, line.Length)
	}

	fmt.Println("Size of file:", len(fileContents))
}
