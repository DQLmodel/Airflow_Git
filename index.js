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
const dqlabs_configurable_keys = core.getInput("dqlabs_configurable_keys") || "";

// Safe array processing utility
const safeArray = (maybeArray) => Array.isArray(maybeArray) ? maybeArray : [];

// Parse configurable keys
const parseConfigurableKeys = (keysString) => {
  if (!keysString || typeof keysString !== 'string') {
    return {
      showFiles: true,
      showJob: true,
      showDirectlyAssetCount: true,
      showDirectlyAssetList: true,
      showIndirectlyAssetCount: true,
      showIndirectlyAssetList: true,
      showSummaryOfImpacts: true
    };
  }

  const keys = keysString.split(',').map(key => key.trim().toLowerCase());
  
  return {
    showFiles: keys.includes('files'),
    showJob: keys.includes('job'),
    showDirectlyAssetCount: keys.includes('directly_asset_count'),
    showDirectlyAssetList: keys.includes('directly_asset_list'),
    showIndirectlyAssetCount: keys.includes('indirectly_asset_count'),
    showIndirectlyAssetList: keys.includes('indirectly_asset_list'),
    showSummaryOfImpacts: keys.includes('summary_of_impacts')
  };
};

// Parse the configurable keys
const configurableKeys = parseConfigurableKeys(dqlabs_configurable_keys);

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

// Generate comprehensive JSON file with all data
const generateComprehensiveJSON = (fileImpacts, changedFiles, matchedJobs) => {
  const jsonData = {
    metadata: {
      timestamp: new Date().toISOString(),
      commit_sha: github.context.sha,
      pull_request_number: github.context.payload.pull_request?.number || null,
      configurable_keys_used: dqlabs_configurable_keys ? dqlabs_configurable_keys.split(',').map(k => k.trim()) : [],
      dqlabs_base_url: dqlabs_base_url,
      analysis_type: "airflow_impact_analysis"
    },
    changed_files: changedFiles,
    asset_impacts: {
      direct: [],
      indirect: []
    },
    summary: {
      total_direct_assets: 0,
      total_indirect_assets: 0,
      total_changed_files: changedFiles.length,
      total_jobs_matched: matchedJobs.length
    }
  };

  // Process file impacts
  Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
    impacts.direct.forEach(model => {
      const redirectUrl = constructItemUrl(model, dqlabs_createlink_url);
      jsonData.asset_impacts.direct.push({
        file_path: filePath,
        model_name: model.name,
        job_name: impacts.jobName,
        redirect_url: redirectUrl
      });
    });

    impacts.indirect.forEach(model => {
      const redirectUrl = constructItemUrl(model, dqlabs_createlink_url);
      jsonData.asset_impacts.indirect.push({
        file_path: filePath,
        model_name: model.name,
        job_name: impacts.jobName,
        redirect_url: redirectUrl
      });
    });
  });

  // Calculate summary totals
  jsonData.summary.total_direct_assets = jsonData.asset_impacts.direct.length;
  jsonData.summary.total_indirect_assets = jsonData.asset_impacts.indirect.length;

  return JSON.stringify(jsonData, null, 2);
};

// Build the new configurable report structure
const buildConfigurableReport = (fileImpacts, changedFiles, matchedJobs) => {
  let report = "## Airflow DAG Impact Analysis Report\n\n";
  
  // 1. Changed Files section (conditional)
  if (configurableKeys.showFiles) {
    report += "### Changed Files\n";
    if (changedFiles.length > 0) {
      changedFiles.forEach(file => {
        report += `- ${file}\n`;
      });
    } else {
      report += "- No files changed\n";
    }
    report += "\n";
  }
  
  // 2. Job information section (conditional)
  if (configurableKeys.showJob && matchedJobs.length > 0) {
    report += "### Matched Jobs\n";
    matchedJobs.forEach(job => {
      report += `- **${job.name}** (${job.filePath})\n`;
    });
    report += "\n";
  }
  
  // 3. Asset level Impacts section (conditional)
  const hasAssetKeys = configurableKeys.showDirectlyAssetCount || configurableKeys.showIndirectlyAssetCount || 
                       configurableKeys.showDirectlyAssetList || configurableKeys.showIndirectlyAssetList;
  
  if (hasAssetKeys) {
    report += "### Asset level Impacts\n";
    
    // Calculate totals
    const totalDirectAssets = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
    const totalIndirectAssets = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
    
    // Show count keys first
    if (configurableKeys.showDirectlyAssetCount) {
      report += `- **Total Directly Impacted:** ${totalDirectAssets}\n`;
    }
    if (configurableKeys.showIndirectlyAssetCount) {
      report += `- **Total Indirectly Impacted:** ${totalIndirectAssets}\n`;
    }
    
    // Show list keys second (as collapsible sections)
    if (configurableKeys.showDirectlyAssetList) {
      const directAssets = [];
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        impacts.direct.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          const modelName = model?.name || 'Unknown';
          if (model?.connection_id && url !== "#") {
            directAssets.push(`- [${modelName}](${url})`);
          } else {
            directAssets.push(`- ${modelName}`);
          }
        });
      });
      
      if (directAssets.length > 0) {
        report += `\n<details>\n<summary><b>Directly Impacted Assets (${directAssets.length})</b></summary>\n\n`;
        report += directAssets.join('\n') + '\n';
        report += `</details>\n`;
      }
    }
    
    if (configurableKeys.showIndirectlyAssetList) {
      const indirectAssets = [];
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        impacts.indirect.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          const modelName = model?.name || 'Unknown';
          if (model?.connection_id && url !== "#") {
            indirectAssets.push(`- [${modelName}](${url})`);
          } else {
            indirectAssets.push(`- ${modelName}`);
          }
        });
      });
      
      if (indirectAssets.length > 0) {
        report += `\n<details>\n<summary><b>Indirectly Impacted Assets (${indirectAssets.length})</b></summary>\n\n`;
        report += indirectAssets.join('\n') + '\n';
        report += `</details>\n`;
      }
    }
    
    report += "\n";
  }
  
  return report;
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


    // Build the configurable report
    summary = buildConfigurableReport(fileImpacts, changedFiles, matchedJobs);

    // Add summary of impacts (conditional)
    if (configurableKeys.showSummaryOfImpacts) {
      const totalDirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
      const totalIndirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
      
      summary += `\n## Summary of Impacts\n`;
      summary += `- **Total Directly Impacted:** ${totalDirect}\n`;
      summary += `- **Total Indirectly Impacted:** ${totalIndirect}\n`;
      summary += `- **Files Changed:** ${Object.keys(fileImpacts).length}\n`;
      summary += `- **Jobs Matched:** ${matchedJobs.length}\n`;
    }

    // Generate comprehensive JSON data
    const comprehensiveJsonData = generateComprehensiveJSON(fileImpacts, changedFiles, matchedJobs);

    // Post or update comment
    if (github.context.payload.pull_request) {
      try {
        const octokit = github.getOctokit(githubToken);
        const { owner, repo } = github.context.repo;
        const issue_number = github.context.payload.pull_request.number;
        
        // Get existing comments to find our bot's comment
        const comments = await octokit.rest.issues.listComments({
          owner,
          repo,
          issue_number,
        });
        
        // Find existing comment from github-actions[bot] with our impact analysis
        const existingComment = comments.data.find(comment => 
          comment.user.type === 'Bot' && 
          comment.user.login === 'github-actions[bot]' &&
          comment.body.includes('## Airflow DAG Impact Analysis Report')
        );
        
        // Add JSON data as collapsible section
        let finalSummary = summary;
        finalSummary += "\n### 📎 Complete Impact Analysis Data\n";
        finalSummary += `<details>\n<summary><b>View Complete JSON Data</b></summary>\n\n`;
        finalSummary += "```json\n";
        finalSummary += comprehensiveJsonData;
        finalSummary += "\n```\n\n";
        finalSummary += "*This JSON contains all impact analysis data regardless of display preferences.*\n";
        finalSummary += `</details>\n\n`;
        
        // Create or update comment with the JSON data
        if (existingComment) {
          core.info(`Updating existing comment ${existingComment.id} with JSON data`);
          await octokit.rest.issues.updateComment({
            owner,
            repo,
            comment_id: existingComment.id,
            body: finalSummary,
          });
          core.info('Successfully updated existing impact analysis comment');
        } else {
          core.info('Creating new impact analysis comment with JSON data');
          await octokit.rest.issues.createComment({
            owner,
            repo,
            issue_number,
            body: finalSummary,
          });
          core.info('Successfully created new impact analysis comment');
        }
        
      } catch (error) {
        core.error(`Failed to post/update comment: ${error.message}`);
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
