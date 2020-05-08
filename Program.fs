open System
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.DependencyInjection
open Giraffe
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Logging
open DataStore
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Buffers
open Microsoft.FSharp.Core
open System.IO
open System.IO.Pipelines
open System.Collections.Generic
open System.Text
open System.Diagnostics
open System.Xml
open Giraffe.Serialization
open System.Globalization
open System.Security.Cryptography

let parseStringToInt (strValue : string) = 
    match strValue |> Int32.TryParse with
    | true, out -> Some out
    | false, _ -> None

let isPathStyle (req : HttpRequest) =
    let host = req.Host.Host

    match ["localhost"] |> List.tryFind (fun h -> host.EndsWith("." + h)) with
    | Some _ -> false
    | None -> true
    
let getBucketName isPathStyle (req: HttpRequest) =
    if isPathStyle then
        match req.Path.Value.Split("/", StringSplitOptions.RemoveEmptyEntries) |> Array.toList with
        | x :: _ -> x |> Some
        | _ -> None
    else
        let host = req.Host.Host
        match host.Split(".") |> Array.toList with
        | x :: _ -> x |> Some
        | _ -> None
        
let getPathElements isPathStyle (path : string) =
    match isPathStyle, path.Split("/", StringSplitOptions.RemoveEmptyEntries) |> Array.toList with
    | true, _ :: elements -> elements 
    | false, elements -> elements 
    | _, _ -> []

let extractKeys (ctx : HttpContext) =
    let key = ctx.TryGetRequestHeader("x-amz-meta-x-amz-key")
    let iv = ctx.TryGetRequestHeader("x-amz-meta-x-amz-iv")

    Option.bind(fun k -> 
        Option.bind(fun i -> 
            {Key = k; IV = i} |> Some) iv) key

let (|Query|_|) key (ctx : HttpContext) = 
    ctx.TryGetQueryStringValue key

let listBuckets : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            let! buckets = store.ListBuckets()
            return! Successful.ok (xml buckets) next ctx
        }

let listBucket bucket : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()

            match ctx with
            | Query "list-type" x when x = "2" ->
                let query = {
                    ContinuationToken = ctx.TryGetQueryStringValue("continuation-token")
                    Prefix = ctx.TryGetQueryStringValue("prefix")
                    MaxKeys = ctx.TryGetQueryStringValue("max-keys") |> Option.bind (parseStringToInt)
                    Delimiter = ctx.TryGetQueryStringValue("delimiter")
                    StartAfter = ctx.TryGetQueryStringValue("start-after")
                }

                let! objects = store.ListBucketV2 bucket query

                return! Successful.ok (xml objects) next ctx
            | _ ->
                let query = {
                    Marker = ctx.TryGetQueryStringValue("marker")
                    Prefix = ctx.TryGetQueryStringValue("prefix")
                    MaxKeys = ctx.TryGetQueryStringValue("max-keys") |> Option.bind (parseStringToInt)
                    Delimiter = ctx.TryGetQueryStringValue("delimiter")
                }

                let! objects = store.ListBucket bucket query

                return! Successful.ok (xml objects) next ctx

        }

let getObject bucket object : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->

        task {
            let store = ctx.GetService<IDataStore>()
            let! meta = store.GetObjectMeta bucket object
            match meta.EncryptionInfo with
            | None -> ()
            | Some ei ->
                ctx.SetHttpHeader "x-amz-meta-x-amz-key" ei.Key
                ctx.SetHttpHeader "x-amz-meta-x-amz-iv" ei.IV
            let! stream = store.GetObject bucket object
            ctx.Response.ContentLength <- Nullable<int64>(stream.Length)
            let mem = ctx.Response.BodyWriter.AsStream()
            do! stream.CopyToAsync(mem)

            return! next ctx
        }

let handleGet bucket elements : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        match bucket, elements with
        | Some bucket, [] ->listBucket bucket next ctx
        | Some bucket, x ->getObject bucket (String.Join("/", x)) next ctx

        | None, [] -> listBuckets next ctx
        | _ ->
            RequestErrors.BAD_REQUEST "" next ctx

let calculateUploadId () =
    Guid.NewGuid().ToString("N")

let calculateMultiPartPath (object : string) uploadId =
    let dir = Path.GetDirectoryName(object)
    let file = Path.GetFileName(object)
    Path.Combine(dir, uploadId, file)

let createMultiPartUpload bucket (object : string) : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) -> task {
        let store = ctx.GetService<IDataStore>()
        let uploadId = calculateUploadId()
        let path = calculateMultiPartPath object uploadId
        let storageClass = ctx.TryGetRequestHeader("x-amz-storage-class") |> Option.defaultValue "STANDARD"
        let key = ctx |> extractKeys
        do! store.InitMultiPartUpload bucket path storageClass key
        let multi = {Bucket = bucket; Key = object; UploadId = uploadId}
        return! Successful.ok (xml multi) next ctx
        }

let completeMultiPartUpload bucket object uploadId : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) -> task {
        let store = ctx.GetService<IDataStore>()
        let! parts = ctx.BindXmlAsync<DataStore.CompleteMultipartUpload>()
        let multiparts = calculateMultiPartPath object uploadId
        let! complete = store.CompleteMultiPartUpload bucket multiparts uploadId parts
        return! Successful.ok (xml complete) next ctx
        }

let deleteObjects bucket : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        printfn "Delete Objects in bucket: %s" bucket
        next ctx


let handlePost bucket elements : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        match ctx.Request.Path.Value, ctx, bucket with
        | "/", Query "delete" _, Some bucket -> deleteObjects bucket next ctx
        | _, Query "uploads" _, Some bucket -> 
            match elements with
            | [] -> RequestErrors.BAD_REQUEST "" next ctx
            | elements -> 
                createMultiPartUpload bucket (String.Join("/", elements)) next ctx
        | _, Query "UploadId" id, Some bucket ->
            match elements with
            | [] -> RequestErrors.BAD_REQUEST "" next ctx
            | elements -> completeMultiPartUpload bucket (String.Join("/", elements)) id next ctx
        | _, _ , _-> 
            RequestErrors.BAD_REQUEST "" next ctx


let createBucket bucket : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            do! store.CreateBucket bucket

            return! next ctx
        }

let copyObject (source : string) bucket object : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            match source.Split("/", StringSplitOptions.RemoveEmptyEntries ) |> Array.toList with
            | x :: e ->
                do! store.CopyObject x (String.Join("/", e)) bucket object
            | _ ->
                ctx.SetStatusCode(500)

            return! next ctx
        }

let inline (|Header|_|) key parse (ctx: HttpContext) =
    match ctx.TryGetRequestHeader key with
    | Some str -> 
        match str |> parse with
        | true, value -> value |> Some
        | _, _ -> None

    | None -> None

let writeBufferOfSpecificLength (reader : PipeReader) (buffer : ReadOnlySequence<byte>) (stream : Stream) length =
    task {
        let (b, written) = 
            match buffer.Length > length with
            | true -> 
                let a = buffer.Slice(buffer.Start, length)
                a, a.Length
            | _ ->
                buffer, buffer.Length

        let mutable pos = b.Start
        let mutable mem = ReadOnlyMemory.Empty

        while b.TryGet(&pos, &mem, true) do
            do! stream.WriteAsync(mem)

        reader.AdvanceTo(b.End)

        return written
    }

let copyReaderToStream (reader : PipeReader) (stream : Stream) length = task {
    
    let mutable run = true
    let mutable l = length
    while run do
        let! result = reader.ReadAsync()

        match l > 0L with
        | true when result.Buffer.IsSingleSegment && result.IsCompleted -> 
            let slice = result.Buffer.Slice(0L, l)
            do! stream.WriteAsync(slice.First)
            reader.AdvanceTo(slice.End)
            run <- false
        | true -> 
            let! bytesWritten = writeBufferOfSpecificLength reader result.Buffer stream l
            let bytesToWrite = l - bytesWritten
            if bytesToWrite <= 0L then 
                run <- false
            else
                l <- bytesToWrite
            
        | _ -> run <- false
    } 

type LengthResult =
    | Value of int64
    | InsufficientLength

//14000;chunk-signature=d4b02e1f393ce0aaaeb8a12b72b21335c0f64f1722d4fc7111b87f5a41d8003e\r\n
let extractLength (reader : PipeReader) (buffer : ReadOnlySequence<byte>) =
    if buffer.IsEmpty then
        printfn "Buffer is empty"

        InsufficientLength
    else
        let sizePos = buffer.PositionOf((byte) ';')
        match sizePos.HasValue with
        | true ->
            let sizeSlice = buffer.Slice(0, sizePos.Value)

            let string = Encoding.UTF8.GetString(sizeSlice.ToArray()).Trim()

            let signature = buffer.Slice(sizeSlice.End)
            let signaturePos = signature.PositionOf((byte)'\n')

            if signaturePos.HasValue then
                reader.AdvanceTo(buffer.GetPosition(1L, signaturePos.Value))

                Convert.ToInt64(string, 16) |> Value
            else 
                InsufficientLength

        | false -> 
            InsufficientLength

let writeBodyToStream (reader : PipeReader) expectedLength (stream : Stream) = task {
    let mutable run = true
    while run do
        let! result = reader.ReadAsync()
        let length = extractLength reader result.Buffer
        match length with
        | Value v when v > 0L ->
            do! copyReaderToStream reader stream v
        | InsufficientLength ->
            reader.AdvanceTo(result.Buffer.Start)
        | _ -> run <- false
    }
    

let storeObject bucket object : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            //TODO check if bucket exists and return erro
            match ctx  with
            | Header "x-amz-decoded-content-length" Int64.TryParse length ->
                let storageClass = ctx.TryGetRequestHeader("x-amz-storage-class") |> Option.defaultValue "STANDARD"
                let store = ctx.GetService<IDataStore>()
                if length = 0L then printfn "Length is 0"
                let reader = ctx.Request.BodyReader
                let func = writeBodyToStream reader length

                let key = ctx |> extractKeys

                let! hash = 
                    match storageClass with
                    | "DEEP_ARCHIVE" -> store.StoreObjectDeep bucket object key func
                    | _ -> store.StoreObjectStandard bucket object key func

                do! reader.CompleteAsync()
                reader.CancelPendingRead()
                ctx.SetHttpHeader "ETag" hash
                return! next ctx
            | Header "Content-Length" Int64.TryParse length ->
                let storageClass = ctx.TryGetRequestHeader("x-amz-storage-class") |> Option.defaultValue "STANDARD"
                let store = ctx.GetService<IDataStore>()
                if length = 0L then printfn "Length is 0"
                let reader = ctx.Request.BodyReader
                let func = fun stream -> copyReaderToStream reader stream length

                let key = ctx |> extractKeys

                let! hash = 
                    match storageClass with
                    | "DEEP_ARCHIVE" -> store.StoreObjectDeep bucket object key func
                    | _ -> store.StoreObjectStandard bucket object key func

                do! reader.CompleteAsync()
                reader.CancelPendingRead()
                ctx.SetHttpHeader "ETag" hash
                return! next ctx


            | _ -> return! RequestErrors.BAD_REQUEST "" next ctx
        }

let generateObjectName objectName (ctx : HttpContext) =
    match ctx, ctx with
    | Query "uploadId" id, Query "partNumber" number ->
        let object = sprintf "%s_%s" objectName number
        calculateMultiPartPath object id
    | _, _ -> objectName

let handlePut bucket elements : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        let headers = ctx.Request.Headers
        
        match ctx.Request.Path.Value, bucket with
        | "/", Some bucket -> createBucket bucket next ctx
        | _, Some bucket ->
            match elements with
            | [] -> createBucket bucket next ctx
            | x ->           
                let objectName = generateObjectName (String.Join("/", x)) ctx
                match headers.TryGetValue("x-amz-copy-source") with
                | true, value when value.Count = 1 -> copyObject value.[0] bucket objectName next ctx
                | _, _ -> 
                    storeObject bucket objectName next ctx

        | _, _ -> RequestErrors.BAD_REQUEST "" next ctx

let deleteBucket bucket : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            do! store.DeleteBucket bucket
            return! next ctx
        }

let deleteObject bucket object : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            do! store.DeleteObject bucket object
            return! next ctx
        }

let abortMultiPart bucket object uploadId : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            let path = calculateMultiPartPath object uploadId
            do! store.AbortMultiPartUpload bucket path uploadId
            return! next ctx
        }

let handleDelete bucket elements : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        printfn "%A %A" bucket elements
        match ctx, bucket with
        | Query "UploadId" id, Some bucket ->
            match elements with
            | [] -> RequestErrors.BAD_REQUEST "" next ctx
            | x -> abortMultiPart bucket (String.Join("/", x)) id next ctx
        | _, Some bucket ->
            match elements with
            | [] -> deleteBucket bucket next ctx
            | x -> deleteObject bucket (String.Join("/", x)) next ctx

        | _, None ->RequestErrors.BAD_REQUEST "" next ctx
  

let getObjectMeta bucket object : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        task {
            let store = ctx.GetService<IDataStore>()
            let! meta = store.GetObjectMeta bucket object
            ctx.Response.ContentLength <- Nullable<int64>(meta.ContentLength)
            ctx.SetHttpHeader "ETag" meta.ETag
            ctx.SetHttpHeader "Last-Modified" (meta.LastModified.ToString("r", DateTimeFormatInfo.InvariantInfo))
            ctx.SetHttpHeader "x-amz-storage-class" meta.StorageClass

            return! next ctx
        }

let handleHead bucket elements : HttpHandler =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        match bucket with
        | Some bucket ->
            match elements with
            | [] -> RequestErrors.BAD_REQUEST "" next ctx
            | x -> getObjectMeta bucket (String.Join("/", x)) next ctx

        | None ->RequestErrors.BAD_REQUEST "" next ctx

let webApp =
    fun (next : HttpFunc) (ctx : HttpContext) ->
        let pathStyle = isPathStyle ctx.Request
        let bucket = getBucketName pathStyle ctx.Request 
        let elements = getPathElements pathStyle ctx.Request.Path.Value 

        printfn "req %s -> %s" ctx.Request.Method ctx.Request.Path.Value

        choose [
            GET >=> routex "/(.*)" >=> warbler (fun _ -> handleGet bucket elements)
            POST >=> routex "/(.*)" >=> warbler (fun _ -> handlePost bucket elements)
            PUT >=> routex "/(.*)" >=> warbler (fun _ -> handlePut bucket elements)
            DELETE >=> routex "/(.*)" >=> warbler (fun _ -> printfn "bla"; handleDelete bucket elements)
            HEAD >=> routex "/(.*)" >=> warbler (fun _ -> handleHead bucket elements)
            ] next ctx

let errorHandler (ex : Exception) (logger : ILogger) =
    logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
    clearResponse
    >=> ServerErrors.INTERNAL_ERROR ex.Message

let configureApp (app : IApplicationBuilder) =
    // Add Giraffe to the ASP.NET Core pipeline
    app.UseGiraffeErrorHandler(errorHandler)
        .UseGiraffe webApp
        

let configureServices (args : string array) (services : IServiceCollection) =
    // Add Giraffe dependencies
    services.AddGiraffe() |> ignore
    let customXmlSettings = XmlWriterSettings()
    customXmlSettings.Encoding <- Encoding.UTF8
    customXmlSettings.NamespaceHandling <- NamespaceHandling.OmitDuplicates
    customXmlSettings.NewLineOnAttributes <- false
    services.AddSingleton<IXmlSerializer>(DefaultXmlSerializer(customXmlSettings)) |> ignore
    services.AddSingleton<IDataStore, FileDataStore>(fun _ -> FileDataStore(args.[0])) |> ignore

let configureLogging (builder : ILoggingBuilder) =
    let filter (l: LogLevel) = l.Equals LogLevel.Error
    builder.AddFilter(filter).AddConsole().AddDebug() |> ignore

    

[<EntryPoint>]
let main args =

    WebHostBuilder()
        .UseUrls("http://*:5000", "http://0.0.0.0:5000" )
        .UseKestrel(fun opts -> opts.Limits.MinRequestBodyDataRate <- null)
        .Configure(Action<IApplicationBuilder> configureApp)
        .ConfigureServices(configureServices args)
        .ConfigureLogging(configureLogging)
        .Build()
        .Run()
    0 // return an integer exit code
