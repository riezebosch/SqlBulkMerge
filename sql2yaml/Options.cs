using CommandLine;

namespace sql2yaml;

public class Options
{
    [Option('c', "connection-string", Required = true)]
    public string ConnectionString { get; set; } = string.Empty;
    
    [Verb("import")]
    public class ImportOptions : Options
    {
        [Option('f', "file", HelpText = "List of files to import.")]
        public IEnumerable<string> Files { get; set; } = [];
    }
    
    
    [Verb("export", HelpText = "export tables to yaml")]
    public class ExportOptions : Options
    {
        [Option('t', "table", HelpText = "List of tables to include in the export.")]
        public IEnumerable<string> Tables { get; set; } = [];

        [Option('d', "directory", HelpText = "Directory to output the yaml files.")]
        public string Directory { get; set; } = string.Empty;
    }
}