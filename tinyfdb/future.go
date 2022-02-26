package tinyfdb

type futureBase struct {
}

func (futureBase) BlockUntilReady() {}
func (futureBase) IsReady() bool    { return true }
func (futureBase) Cancel()          {}

type futureByteSlice struct {
	futureBase

	err error
	bs  []byte
}

func (f *futureByteSlice) Get() ([]byte, error) {
	return f.bs, f.err
}

func (f *futureByteSlice) MustGet() []byte {
	bs, err := f.Get()
	if err != nil {
		panic(err)
	}
	return bs
}

type futureNil struct {
	futureBase

	err error
}

func (f *futureNil) Get() error {
	return f.err
}

func (f *futureNil) MustGet() {
	err := f.Get()
	if err != nil {
		panic(err)
	}
}
