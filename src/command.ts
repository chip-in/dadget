import { ResourceNode } from "@chip-in/resource-node";
import * as commandLineArgs from "command-line-args";
import * as commandLineUsage from "command-line-usage";
import { Maintenance } from "./Maintenance";
import Dadget from "./se/Dadget";

const usage = () => {
  const sections = [
    {
      header: "Command List",
      content: [
        { name: "reset", summary: "Reset local DB data." },
        { name: "export", summary: "Export DB data." },
        { name: "import", summary: "Import DB data." },
        { name: "backup", summary: "Backup DB data." },
        { name: "restore", summary: "Restore DB data." },
      ],
    },
    {
      header: "reset option",
      optionList: [
        {
          name: "name",
          typeLabel: "{underline DB name}",
        },
      ],
    },
    {
      header: "export option",
      optionList: [
        {
          name: "server",
          typeLabel: "{underline core server}",
        },
        {
          name: "rn",
          typeLabel: "{underline Resource Node name}",
        },
        {
          name: "name",
          typeLabel: "{underline database name}",
        },
        {
          name: "file",
          typeLabel: "{underline output file}",
        },
      ],
    },
    {
      header: "import option",
      optionList: [
        {
          name: "server",
          typeLabel: "{underline core server}",
        },
        {
          name: "rn",
          typeLabel: "{underline Resource Node name}",
        },
        {
          name: "name",
          typeLabel: "{underline database name}",
        },
        {
          name: "id",
          typeLabel: "{underline id column name}",
        },
        {
          name: "file",
          typeLabel: "{underline input file}",
        },
      ],
    },
    {
      header: "backup option",
      optionList: [
        {
          name: "server",
          typeLabel: "{underline core server}",
        },
        {
          name: "rn",
          typeLabel: "{underline Resource Node name}",
        },
        {
          name: "name",
          typeLabel: "{underline database name}",
        },
        {
          name: "file",
          typeLabel: "{underline output file}",
        },
      ],
    },
    {
      header: "restore option",
      optionList: [
        {
          name: "server",
          typeLabel: "{underline core server}",
        },
        {
          name: "rn",
          typeLabel: "{underline Resource Node name}",
        },
        {
          name: "name",
          typeLabel: "{underline database name}",
        },
        {
          name: "file",
          typeLabel: "{underline input file}",
        },
      ],
    },
  ];
  const usage = commandLineUsage(sections);
  console.info(usage);
  process.exit();
};

const mainDefinitions = [
  { name: "command", defaultOption: true },
];
const mainOptions = commandLineArgs(mainDefinitions, { stopAtFirstUnknown: true });
const argv = mainOptions._unknown || [];

if (mainOptions.command === "reset") {
  const argsDefinitions = [
    { name: "target", defaultOption: true },
    { name: "name" },
  ];
  const options = commandLineArgs(argsDefinitions, { argv });
  const target = options.target || options.name;
  if (!target) {
    usage();
  }
  Maintenance.reset(target);

} else if (mainOptions.command === "export" || mainOptions.command === "import" ||
  mainOptions.command === "backup" || mainOptions.command === "restore") {
  const argsDefinitions = [
    { name: "server" },
    { name: "rn" },
    { name: "name" },
    { name: "file" },
    { name: "id" },
  ];
  const options = commandLineArgs(argsDefinitions, { argv });
  if (!options.server || !options.rn || !options.name || !options.file) {
    usage();
  }
  if (mainOptions.command === "import" && !options.id) {
    usage();
  }
  Dadget.enableDeleteSubset = false;
  const node = new ResourceNode(options.server, options.rn);
  Dadget.registerServiceClasses(node);
  node.start().then(() => {
    const seList = node.searchServiceEngine("Dadget", { database: options.name });
    if (seList.length === 0) {
      return Promise.reject("RNの構成にDadgetがないか、データベース名を間違っています。");
    }
    const dadget = seList[0] as Dadget;
    if (mainOptions.command === "export") {
      return Maintenance.export(dadget, options.file);
    } else if (mainOptions.command === "import") {
      return Maintenance.import(dadget, options.file, options.id);
    } else if (mainOptions.command === "backup") {
      return Maintenance.export(dadget, options.file);
    } else if (mainOptions.command === "restore") {
      return Maintenance.restore(dadget, options.file);
    }
  })
    .then(() => node.stop().then(() => process.exit()))
    .catch((msg) => {
      console.error(msg.toString ? msg.toString() : msg);
      node.stop()
        .then(() => process.exit());
    });
} else {
  usage();
}
