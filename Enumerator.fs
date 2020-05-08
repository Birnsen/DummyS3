module Enumerator

open System.Collections.Generic

type 't EnumeratorPlus(enumerator : IEnumerator<'t>) as __ =
    let mutable hasEnded = false
    
    member __.HasEnded with get() = hasEnded

    interface IEnumerator<'t> with
        member __.Current: obj = 
            enumerator.Current  :> obj

        member __.Current: 't = 
            enumerator.Current

        member __.Dispose(): unit = 
            enumerator.Dispose()

        member __.MoveNext(): bool = 
            let moved = enumerator.MoveNext()
            if not moved then hasEnded <- true
            moved

        member __.Reset(): unit = 
            enumerator.Reset()
            hasEnded <- false

    member __.GetEnumerable () = 
        let this = __ :> IEnumerator<'t>
        seq {
            while this.MoveNext() do
                yield this.Current
        }
        
