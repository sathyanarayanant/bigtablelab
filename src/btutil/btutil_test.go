package btutil

import "testing"

func TestGet7CharStrLeadingZeros(t *testing.T) {
	const fn = "TestGet7CharStrLeadingZeros"

	type test struct {
		input string
		n int
		expectedOutput string
		expectedError bool
	}

	tests := []test {
		{"100", 7, "0000100", false},
		{"4094961", 7, "4094961", false},
		{"60", 4, "0060", false},
		{"zycadazycada", 7, "", true},
	}

	for _, e := range tests {
		output, err := GetNCharStrLeadingZeros(e.input, e.n)
		isError := err != nil

		if isError != e.expectedError {
			t.Errorf("%v: fail for [%v], expected error [%v], got [%v]", fn, e.input, e.expectedError, isError)
		}
		if output != e.expectedOutput {
			t.Errorf("%v: fail for [%v], expected output [%v], got [%v]", fn, e.input, e.expectedOutput, output)
		}
	}
}
