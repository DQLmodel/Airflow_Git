const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");

// Get inputs with defaults
const clientId = core.getInput("api_client_id") || "";
const clientSecret = core.getInput("api_client_secret") || "";
const changedFilesList = core.getInput("changed_files_list") || "";
const githubToken = core.getInput("github_token") || "";
console.log("Github token", githubToken);
const dqlabs_base_url = core.getInput("dqlabs_base_url") || "";
console.log("base url", dqlabs_base_url);
const dqlabs_createlink_url = core.getInput("dqlabs_createlink_url") || "";

// Safe array processing utility
const safeArray = (maybeArray) => Array.isArray(maybeArray) ? maybeArray : [];

const getChangedFiles = async () => {
  try {
    if (changedFilesList && typeof changedFilesList === "string") {
      return changedFilesList
        .split(",")
        .map(f => typeof f === "string" ? f.trim() : "")
        .filter(f => f && f.length > 0);
    }

    const eventPath = process.env.GITHUB_EVENT_PATH;
    if (!eventPath) return [];

    const eventData = JSON.parse(fs.readFileSync(eventPath, "utf8"));
    const changedFiles = new Set();

    const commits = safeArray(eventData.commits);
    commits.forEach(commit => {
      if (!commit) return;
      const files = [
        ...safeArray(commit.added),
        ...safeArray(commit.modified),
        ...safeArray(commit.removed)
      ];
      files.filter(Boolean).forEach(file => changedFiles.add(file));
    });

    return Array.from(changedFiles);
  } catch (error) {
    core.error(`[getChangedFiles] Error: ${error.message}`);
    return [];
  }
};

const getAirflowJobs = async () => {
  try {
    const jobUrl = `${dqlabs_base_url}/api/pipeline/job/`;
    console.log("Job url", jobUrl);
    const payload = {
      chartType: 0,
      search: {},
      page: 0,
      pageLimit: 100,
      sortBy: "name",
      orderBy: "asc",
      date_filter: { days: "All", selected: "All" },
      chart_filter: {},
      is_chart: true,
    };

    const response = await axios.post(jobUrl, payload, {
      headers: {
        "Content-Type": "application/json",
        "client-id": clientId,
        "client-secret": clientSecret,
      }
    });


    return response?.data?.response?.data || [];
  } catch (error) {
    core.error(`[getAirflowJobs] Error: ${error.message}`);
    return [];
  }
};

const getImpactAnalysisData = async (asset_id, connection_id, entity, isDirect = true) => {
  try {
    const impactAnalysisUrl = `${dqlabs_base_url}/api/lineage/impact-analysis/`;
    const payload = {
      connection_id,
      asset_id,
      entity,
      moreOptions: {
        view_by: "table",
        ...(!isDirect && { depth: 3 })
      },
      search_key: ""
    };

    const response = await axios.post(
      impactAnalysisUrl,
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          "client-id": clientId,
          "client-secret": clientSecret,
        },
      }
    );
    return response?.data?.response?.data || {};
    // return safeArray(response?.data?.response?.data?.tables || []), safeArray(response?.data?.response?.data?.response?.indirect || []);
  } catch (error) {
    core.error(`[getImpactAnalysisData] Error for ${entity}: ${error.message}`);
    return {};
  }
};


const run = async () => {
  try {
    let summary = "## Airflow DAG Impact Analysis Report\n\n";

    const changedFiles = safeArray(await getChangedFiles());
    console.log("Changed files", changedFiles);
    core.info(`Found ${changedFiles.length} changed files`);

    // Process changed Python files (Airflow DAGs)
    const changedDAGs = changedFiles
      .filter(file => file && typeof file === "string" && file.endsWith(".py"))
      .map(file => path.basename(file, path.extname(file)))
      .filter(Boolean);

    console.log("Changed DAGs", changedDAGs);

    // Get Airflow Jobs from DQLabs (treating DAGs as jobs)
    const jobs = await getAirflowJobs();

    // Analyze each changed DAG file
    const dagAnalyses = [];

    // Match DAGs with jobs and perform impact analysis
    const matchedJobs = jobs
      .filter(job => job?.connection_type === "airflow")
      .filter(job => changedDAGs.includes(job?.name))
      .map(job => ({
        ...job,
        asset_id: job?.asset_id || "",
        entity: job?.asset_id || "",
        filePath: changedDAGs.find(f => path.basename(f, path.extname(f)) === job?.name)
      }))
      .filter(job => job.filePath);

    // Store impacts per file
    const fileImpacts = {};

    // Initialize file impacts structure
    matchedJobs.forEach(job => {
      fileImpacts[job.filePath] = {
        direct: [],
        indirect: [],
        jobName: job.name
      };
    });

    // Process impact data for each job
    for (const job of matchedJobs) {
      // Get direct impacts (without depth)
      const impactData = await getImpactAnalysisData(
        job.asset_id,
        job.connection_id,
        job.entity,
        false // isDirect = true
      );

      const impactTables = impactData?.tables || [];
      const indirectImpact = impactData?.indirect || [];
      const indirectNameSet = new Set(indirectImpact.map(item => item?.name));

      // Filter out the job itself from direct impacts
      const filteredDirectImpact = impactTables
        .filter(table => !indirectNameSet.has(table?.name))
        .filter(Boolean);

      fileImpacts[job.filePath].direct.push(...filteredDirectImpact);

      // Get indirect impacts (with depth=10)
      // const indirectImpact = await getImpactAnalysisData(
      //   job.asset_id,
      //   job.connection_id,
      //   job.entity,
      //   false // isDirect = false
      // );

      fileImpacts[job.filePath].indirect.push(...indirectImpact);
    }

    // Create unique key function for comparison
    const uniqueKey = (item) => `${item?.name}-${item?.connection_id}-${item?.asset_name}`;

    // Remove direct impacts from indirect results for each file
    Object.keys(fileImpacts).forEach(filePath => {
      const impacts = fileImpacts[filePath];
      const directKeys = new Set(impacts.direct.map(uniqueKey));
      impacts.indirect = impacts.indirect.filter(
        item => !directKeys.has(uniqueKey(item))
      );
    });

    // Deduplicate results within each file
    const dedup = (arr) => {
      const seen = new Set();
      return arr.filter(item => {
        const key = uniqueKey(item);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    };

    Object.keys(fileImpacts).forEach(filePath => {
      fileImpacts[filePath].direct = dedup(fileImpacts[filePath].direct);
      fileImpacts[filePath].indirect = dedup(fileImpacts[filePath].indirect);
    });

    const constructItemUrl = (item, baseUrl) => {
      if (!item || !baseUrl) return "#";

      try {
        const url = new URL(baseUrl);

        // Handle pipeline items
        if (item.asset_group === "pipeline") {
          if (item.is_transform) {
            url.pathname = `/observe/pipeline/transformation/${item.redirect_id}/run`;
          } else {
            url.pathname = `/observe/pipeline/task/${item.redirect_id}/run`;
          }
          return url.toString();
        }

        // Handle data items
        if (item.asset_group === "data") {
          url.pathname = `/observe/data/${item.redirect_id}/measures`;
          return url.toString();
        }

        // Default case
        return "#";
      } catch (error) {
        core.error(`Error constructing URL for ${item.name}: ${error.message}`);
        return "#";
      }
    };

    // Build the complete impacts section
    const buildImpactsSection = (fileImpacts) => {
      let content = '';
      let totalDirect = 0;
      let totalIndirect = 0;
      
      // Generate content for each file
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        const { direct, indirect, jobName } = impacts;
        totalDirect += direct.length;
        totalIndirect += indirect.length;

        content += `### File: ${filePath}\n`;
        content += `**Job:** ${jobName}\n\n`;
        
        content += `#### Directly Impacted (${direct.length})\n`;
        direct.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          content += `- [${model?.name || 'Unknown'}](${url})\n`;
        });

        content += `\n#### Indirectly Impacted (${indirect.length})\n`;
        indirect.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          content += `- [${model?.name || 'Unknown'}](${url})\n`;
        });

        content += '\n\n';
      });

      const totalImpacts = totalDirect + totalIndirect;
      const shouldCollapse = totalImpacts > 20;

      if (shouldCollapse) {
        return `<details>
<summary><b>Impact Analysis (${totalImpacts} total impacts - ${Object.keys(fileImpacts).length} files changed) - Click to expand</b></summary>

${content}
</details>`;
      }
      
      return content;
    };

    // Add DAG analysis details
    if (dagAnalyses.length > 0) {
      summary += "### Changed Airflow DAGs\n\n";
      
      dagAnalyses.forEach(analysis => {
        if (analysis.error) {
          summary += `#### ${analysis.filePath}\n`;
          summary += `❌ Error analyzing DAG: ${analysis.error}\n\n`;
          return;
        }

        summary += `#### ${analysis.filePath}\n`;
        summary += `- **DAG ID:** ${analysis.dagId || 'Not found'}\n`;
        summary += `- **Schedule:** ${analysis.schedule || 'Not specified'}\n`;
        summary += `- **Description:** ${analysis.description || 'No description'}\n`;
        summary += `- **Tags:** ${analysis.tags.length > 0 ? analysis.tags.join(', ') : 'None'}\n`;
        summary += `- **Tasks:** ${analysis.tasks.length}\n`;
        
        if (analysis.tasks.length > 0) {
          summary += `  - ${analysis.tasks.join(', ')}\n`;
        }
        
        if (analysis.dependencies.length > 0) {
          summary += `- **Dependencies:**\n`;
          analysis.dependencies.forEach(dep => {
            summary += `  - ${dep.from} → ${dep.to}\n`;
          });
        }
        
        summary += "\n";
      });
    }

    // Add impacts to summary
    summary += buildImpactsSection(fileImpacts);

    // Add summary of total impacts
    const totalDirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
    const totalIndirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
    
    summary += `\n## Summary of Impacts\n`;
    summary += `- **Total Directly Impacted:** ${totalDirect}\n`;
    summary += `- **Total Indirectly Impacted:** ${totalIndirect}\n`;
    summary += `- **Files Changed:** ${Object.keys(fileImpacts).length}\n`;
    summary += `- **Jobs Matched:** ${matchedJobs.length}\n`;

    // Post comment
    if (github.context.payload.pull_request) {
      try {
        const octokit = github.getOctokit(githubToken);
        await octokit.rest.issues.createComment({
          owner: github.context.repo.owner,
          repo: github.context.repo.repo,
          issue_number: github.context.payload.pull_request.number,
          body: summary,
        });
      } catch (error) {
        core.error(`Failed to create comment: ${error.message}`);
      }
    }

    // Output results
    await core.summary
      .addRaw(summary)
      .write();

    core.setOutput("impact_markdown", summary);
  } catch (error) {
    core.setFailed(`[MAIN] Unhandled error: ${error.message}`);
    core.error(error.stack);
  }
};

// Execute
run().catch(error => {
  core.setFailed(`[UNCAUGHT] Critical failure: ${error.message}`);
});
