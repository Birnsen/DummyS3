module DataStore

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.IO
open System
open System.Security.Cryptography
open System.Linq
open System.Diagnostics
open System.Xml.Serialization
open System.Text.Json
open System.Text.Json.Serialization
open System.Text
open System.Collections.Concurrent
open System.Collections.Generic
open Enumerator
open System.Runtime.Caching

let jsonOptions = JsonSerializerOptions()
jsonOptions.Converters.Add(JsonFSharpConverter())

type ListBucketQueryParams = {
    Marker: string option
    Prefix: string option
    MaxKeys: int option
    Delimiter: string option
    }

type ListBucketQueryParamsV2 = {
    ContinuationToken: string option
    Delimiter: string option
    MaxKeys: int option
    Prefix: string option
    StartAfter: string option
    }

type S3Bucket = {
    Name: string
    CreationDate: DateTime
    Objects: string list
}

type S3Object = {
    Name: string
    Size: int64
    CreationDate: DateTime
    ModifiedDate: DateTime
    Md5: string
    ContentType: string
    ContentDisposition: string
    ContentEncoding: string
    CustomMetadata: string
    CacheControl: string

    }

[<CLIMutable>]
type Owner = {
    Id : string
    DisplayName : string
}
with
    static member create () =
        {DisplayName = "Peter"; Id = Guid.NewGuid().ToString()}

[<CLIMutable>]
type Bucket = {
    Name : string
    CreationDate : DateTime
}

[<CLIMutable>]
type ListBucketsOutput = {
    Buckets : Collections.Generic.List<Bucket>
    Owner : Owner
}

[<CLIMutable>]
type Object = {
    ETag : string
    Key : string
    LastModified : DateTime
    Owner : Owner
    Size : int
    StorageClass : string
}
with
    static member create (fileInfo : FileSystemInfo) =
        {ETag = "828ef3fdfa96f00ad9f27c383fc9ac7f"; Key = fileInfo.Name; LastModified = fileInfo.LastWriteTime; Owner = Owner.create(); Size = 4711; StorageClass = "STANDARD"}

[<CLIMutable>]
type ListBucketResult = {
    IsTruncated : bool
    Marker : string
    NextMarker : string
    Name : string
    Prefix : string
    Delimiter : string
    MaxKeys : int
    EncodingType : string
    [<XmlElement()>]
    Contents : Collections.Generic.List<Object>
}

[<CLIMutable>]
type ListBucketResultV2 = {
    Name : string
    Prefix : string
    StartAfter : string
    KeyCount : int
    MaxKeys : int
    Delimiter : string
    IsTruncated : bool
    ContinuationToken : string
    NextContinuationToken : string
    EncodingType : string
    [<XmlElement()>]
    Contents : Collections.Generic.List<Object>
}

type EncryptionInfo = {
    Key : string
    IV : string
}

type MetaInfo = {
    ETag : string
    StorageClass : string
    EncryptionInfo : EncryptionInfo option
}

type MetaInfoResponse = {
    ETag : string
    ContentLength : int64
    LastModified : DateTime
    StorageClass : string
    EncryptionInfo : EncryptionInfo option
}

[<CLIMutable>]
type InitiateMultipartUploadResult = {
    Bucket : string
    Key : string
    UploadId : string
}

[<CLIMutable>]
type Part = {
    ETag: string
    PartNumber: int
}

[<CLIMutable>]
type CompleteMultipartUpload = {
    [<XmlElement(ElementName="Part")>]
    Parts : Collections.Generic.List<Part>
}

[<CLIMutable>]
type CompleteMultipartUploadOutput = {
    Location: string
    Bucket: string
    Key: string
    ETag: string
}

type IDataStore =
    abstract ListBuckets: unit -> Task<ListBucketsOutput>
    abstract ListBucketV2: bucket: string -> query: ListBucketQueryParamsV2 -> Task<ListBucketResultV2>
    abstract ListBucket: bucket: string -> query: ListBucketQueryParams -> Task<ListBucketResult>
    abstract CreateBucket: bucket: string -> Task
    abstract DeleteBucket: bucket: string -> Task
    abstract InitMultiPartUpload: bucket: string -> object: string -> storageClass: string -> enc: EncryptionInfo option -> Task
    abstract CompleteMultiPartUpload: bucket: string -> objectName: string -> uploadId: string -> request: CompleteMultipartUpload -> Task<CompleteMultipartUploadOutput>
    abstract AbortMultiPartUpload: bucket: string -> objectName: string -> uploadId: string -> Task
    abstract GetObject: bucket:string -> objectName: string -> Task<Stream>
    abstract GetObjectMeta: bucket:string -> objectName: string -> Task<MetaInfoResponse>
    abstract CopyObject: srcBucket: string -> srcObject: string -> dstBucket: string -> dstObject: string -> Task
    abstract StoreObjectStandard: bucket: string -> objectName: string -> enc: EncryptionInfo option -> (Stream -> Task<unit>) -> Task<string>
    abstract StoreObjectDeep: bucket: string -> objectName: string -> enc: EncryptionInfo option -> (Stream -> Task<unit>) -> Task<string>
    abstract DeleteObject: bucket: string -> objectName: string -> Task
    abstract DeleteObjects: bucket: string -> objectName: string list -> Task

type FileDataStore(basePath: string) =
    
    let root = DirectoryInfo(basePath)
    let uploadIds = new ConcurrentDictionary<string, {|Class: string; Enc: EncryptionInfo option|}>()

    let storeObject filePath (func : (Stream -> Task<unit>)) = task {
    
        let hasher = MD5.Create()
        use dst = new FileStream(filePath, FileMode.Create)
        use md5Stream = new CryptoStream(dst, hasher, CryptoStreamMode.Write)

        let sw = Stopwatch.StartNew()

        do! func md5Stream

        md5Stream.FlushFinalBlock()

        let hash = BitConverter.ToString(hasher.Hash).Replace("-", "").ToLowerInvariant()

        sw.Stop()

        //printfn "Store object %s -> %d" filePath sw.ElapsedMilliseconds
        return hash
        }

    let cache = new MemoryCache("Continuations")

    let policy = CacheItemPolicy()
    do policy.SlidingExpiration <- TimeSpan.FromMinutes(5.0)

    let getEnumerator dir = function
        | Some cont when not (String.IsNullOrEmpty(cont)) && cache.Contains(cont) ->
            let enumerator = cache.Get(cont) :?> EnumeratorPlus<Object>
            (cont, enumerator)
        | _ ->
            let dirs = 
                DirectoryInfo(dir)
                    .EnumerateFiles("*", SearchOption.AllDirectories)
                    .Where(fun f -> f.FullName.EndsWith(".!meta!") |> not)
                    .Select(fun fsi -> Object.create fsi)
                    .GetEnumerator()

            let enumerator = new EnumeratorPlus<Object>(dirs)
            let guid = Guid.NewGuid().ToString("N")
            cache.Add(guid, enumerator, policy) |> ignore
            (guid, enumerator)
            

    interface IDataStore with
        member __.ListBuckets () =
            task {
                let dirs = 
                    root
                        .EnumerateDirectories()
                        .Where(fun d -> d.Name.ToLower().Contains("recycle.bin") |> not)
                        .Where(fun d -> d.Name.ToLower().Contains("system volume information") |> not)
                        .Select(fun d -> {Name = d.Name; CreationDate = d.CreationTime})
                        .ToList()

                return {Buckets = dirs; Owner = {DisplayName = "Peter"; Id = "1234"}}
            }
        member __.ListBucket bucket query =
            task {
                let dir = Path.Combine(root.FullName, bucket)
                let maxKeys = query.MaxKeys |> Option.defaultValue 1000

                let (cont, enumerator) = getEnumerator dir query.Marker

                let files = enumerator.GetEnumerable().Take(maxKeys).ToList()

                let hasEnded = enumerator.HasEnded

                let ret = 
                    { IsTruncated = not hasEnded
                    ; Marker = query.Marker |> Option.defaultValue String.Empty
                    ; NextMarker = if hasEnded then String.Empty else cont
                    ; Name = bucket
                    ; Prefix = query.Prefix |> Option.defaultValue String.Empty
                    ; Delimiter = query.Delimiter |> Option.defaultValue String.Empty
                    ; MaxKeys = maxKeys
                    ; EncodingType = String.Empty
                    ; Contents = files}

                printfn "List Bucket %s" bucket
                return ret
            }
        member __.ListBucketV2 bucket query =
            task {
                let dir = Path.Combine(root.FullName, bucket)
                let maxKeys = query.MaxKeys |> Option.defaultValue 1000

                let (cont, enumerator) = getEnumerator dir query.ContinuationToken

                let files = enumerator.GetEnumerable().Take(maxKeys).ToList()

                let hasEnded = enumerator.HasEnded

                let ret = 
                    { IsTruncated = not hasEnded
                    ; ContinuationToken = query.ContinuationToken |> Option.defaultValue String.Empty
                    ; NextContinuationToken = if hasEnded then String.Empty else cont
                    ; Name = bucket
                    ; Prefix = query.Prefix |> Option.defaultValue String.Empty
                    ; Delimiter = query.Delimiter |> Option.defaultValue String.Empty
                    ; MaxKeys = maxKeys
                    ; KeyCount = files.Count
                    ; EncodingType = String.Empty
                    ; StartAfter = query.StartAfter |> Option.defaultValue String.Empty
                    ; Contents = files}

                printfn "List Bucket %s" bucket
                return ret
            }

        member __.CreateBucket bucket =
            task { 
                let newBucket = root.CreateSubdirectory(bucket)
                printfn "Create bucket %s" newBucket.FullName
                return ()
            } :> Task

        member __.DeleteBucket bucket =
            task { 
                let dir = Path.Combine(root.FullName, bucket)
                Directory.Delete(dir, true)
                printfn "Delete bucket: %s" bucket
                return ()
            } :> Task

        member __.GetObject bucket objectName =
            task { 
                printfn "Get object %s/%s" bucket objectName
                let file = Path.Combine(root.FullName, bucket, objectName)
                return File.OpenRead(file) :> Stream
            }

        member __.GetObjectMeta bucket objectName =
            task { 
                printfn "Get object info %s/%s" bucket objectName
                let file = new FileInfo(Path.Combine(root.FullName, bucket, objectName) + ".!meta!")
                let! meta = JsonSerializer.DeserializeAsync<MetaInfo>(file.OpenRead(), jsonOptions)
                let metaInfo = 
                    { ETag = meta.ETag
                    ; ContentLength = file.Length
                    ; LastModified = file.LastWriteTime
                    ; StorageClass = meta.StorageClass
                    ; EncryptionInfo = meta.EncryptionInfo}

                return metaInfo
            }

        member __.CopyObject srcBucket srcObject dstBucket dstObject = 
            task { 
                printfn "copy object %s/%s -> %s/%s" srcBucket srcObject dstBucket dstObject
                return ()
            } :> Task


        member __.InitMultiPartUpload bucket object storageClass enc = 
            task {
                let file = FileInfo(Path.Combine(root.FullName,bucket, object))
                if not file.Directory.Exists then file.Directory.Create()
                uploadIds.TryAdd(file.Directory.Name, {|Class = storageClass; Enc = enc |} ) |> ignore

                return ()
            } :> Task
            
        member __.AbortMultiPartUpload bucket object uploadId = 
            task {
                let file = FileInfo(Path.Combine(root.FullName,bucket, object))
                if file.Directory.Name <> uploadId then return ()
                file.Directory.Delete(true)
                uploadIds.TryRemove(uploadId) |> ignore

                return ()
            } :> Task

        member __.CompleteMultiPartUpload bucket object uploadId parts = task {
            let sorted = parts.Parts.OrderBy(fun p -> p.PartNumber)
            let file = FileInfo(Path.Combine(root.FullName, bucket, object))
            let baseFile = new FileInfo(Path.Combine(file.Directory.Parent.FullName, file.Name))
            use fileStream = baseFile.OpenWrite()

            let sb = new StringBuilder();
            for p in sorted do
                let partFile = sprintf "%s_%d" file.FullName p.PartNumber
                let metaPartFile = sprintf "%s_%d.!meta!" file.FullName p.PartNumber 
                use metaStream = File.OpenRead(metaPartFile)
                let! meta = JsonSerializer.DeserializeAsync<MetaInfo>(metaStream, jsonOptions)
                sb.Append(meta.ETag) |> ignore
                let! bytes = File.ReadAllBytesAsync(partFile)
                do! fileStream.WriteAsync(bytes, 0, bytes.Length)

            file.Directory.Delete(true)
            let meta = uploadIds.[uploadId]
            let computedHash = MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(sb.ToString()))
            let hash = BitConverter.ToString(computedHash).Replace("-", "").ToLowerInvariant()
            let etag = sprintf "%s-%d" hash (sorted.Count())
            let meta = JsonSerializer.Serialize({ETag = etag; StorageClass = meta.Class; EncryptionInfo = meta.Enc}, jsonOptions)
            do! File.WriteAllTextAsync(baseFile.FullName + ".!meta!", meta)
            uploadIds.TryRemove(uploadId) |> ignore
            return {Location = "Hier"; Bucket = bucket; Key = object; ETag = etag}
            }
            
        
        member __.StoreObjectStandard bucket objectName enc func =
            task {
                let file = FileInfo(Path.Combine(root.FullName, bucket, objectName))
                if not file.Directory.Exists then file.Directory.Create()
                let! hash = storeObject file.FullName func
                let meta = JsonSerializer.Serialize({ETag = hash; StorageClass = "STANDARD"; EncryptionInfo = enc},jsonOptions)
                do! File.WriteAllTextAsync(file.FullName + ".!meta!", meta)

                return hash
            }

        member __.StoreObjectDeep bucket objectName enc func =
            task {
                let file = FileInfo(Path.Combine(root.FullName, bucket, objectName))
                if not file.Directory.Exists then file.Directory.Create()
                let! hash = storeObject file.FullName func
                let meta = JsonSerializer.Serialize({ETag = hash; StorageClass = "DEEP_ARCHIVE"; EncryptionInfo = enc}, jsonOptions)
                do! File.WriteAllTextAsync(file.FullName + ".!meta!", meta)

                return hash
            }

        member __.DeleteObject bucket objectName =
            task {
                let file = Path.Combine(root.FullName, bucket, objectName)
                File.Delete(file)
                File.Delete(file + ".!meta!")
                printfn "Delete Object: %s/%s" bucket objectName
                return ()
                
            } :> Task
        
        member __.DeleteObjects bucket objects = 
            task { 
                let dir = Path.Combine(root.FullName, bucket)

                for o in objects do
                    let file = Path.Combine(dir, o)
                    File.Delete(file)
                
                return ()
                } :> Task




