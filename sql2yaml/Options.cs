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

        [Option(longName: "pre-import-scripts", HelpText = "List of files or directories with SQL scripts to be executed before import.")]
        public IEnumerable<string> PreImportScripts { get; set; } = [];

        [Option(longName: "post-import-scripts", HelpText = "List of files or directorie with SQL scripts to be executed after import.")]
        public IEnumerable<string> PostImportScripts { get; set; } = [];
    }
    
    
    [Verb("export", HelpText = "export tables to yaml")]
    public class ExportOptions : Options
    {
        [Option('t', "tables", HelpText = "List of tables to include in the export.")]
        public IEnumerable<string> Tables { get; private set; } = [];

        [Option('d', "directory", HelpText = "Directory to output the yaml files.")]
        public string Directory { get; set; } = string.Empty;

        [Option(longName: "pre-export-scripts", HelpText = "List of files or directories with SQL scripts to be executed before export.")]
        public IEnumerable<string> PreExportScripts { get; set; } = [];

        [Option(longName: "post-export-scripts", HelpText = "List of files or directories with SQL scripts to be executed after export.")]
        public IEnumerable<string> PostExportScripts { get; set; } = [];
    }
}