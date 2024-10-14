using CommandLine;

namespace sql2yaml;

public class Options
{
    [Option('c', "connection-string", Required = true)]
    public string ConnectionString { get; set; } = string.Empty;
    
    [Verb("import")]
    public class ImportOptions : Options
    {
        [Option('d', "directory", HelpText = "Directory with yaml files.")]
        public string Directory { get; set; } = string.Empty;

        [Option('u', "delete-unmatched", Default = false, HelpText = "Delete records when not matched from yaml.")]
        public bool DeleteUnmatched { get; set; }
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