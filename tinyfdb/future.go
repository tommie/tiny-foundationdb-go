package tinyfdb

type futureBase struct {
}

func (futureBase) BlockUntilReady() {}
func (futureBase) IsReady() bool    { return true }
func (futureBase) Cancel()          {}

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
