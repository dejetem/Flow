# Flow Knowledge Graph (KG)

## Role & Goals

Provides the semantic memory and context foundation for the Flow system, and defines how models and tools are integrated and invoked verifiably.

*   **KG:** Manages entities, relationships, context, causality, and provenance as a distributed graph.
*   **MCP:** Standardizes the description, requirements, invocation, and verification of external models, tools, and functions.

## Foundational Role

*   **KG:** Provides semantic context and long-term memory for agents.
*   **MCP:** Handles the provenance, invocation, and verification of models and tools used by agents and DAG tasks.

## KG Architecture

*   **Data Structure:** IPLD-compatible object graph.
*   **Backend:** CRDT-backed persistence (e.g., using Any-Sync) for decentralized consistency.
*   **Nodes:** Represent entities (users, agents, tasks, data, concepts), events, etc. Identified by CIDs or DIDs.
*   **Edges:** Represent semantic or causal links between nodes.
*   **Layered Model:** Conceptually layered for different types of information:
    *   Entity Layer
    *   Context Layer
    *   Semantic Layer (Schemas, Ontologies)
    *   Causal Layer (Execution Traces)
    *   Provenance Layer (Origin, Signatures, Proofs)
*   **Schemas:** Supports JSON-LD, RDFS/OWL-lite for defining object types and relationships.
*   **Access Control:** VC-based permissions govern read/write access to graph partitions.

## Contextualization & Binding

*   Links KG nodes to:
    *   Agent SLRPA phases (providing context for Sense, Reason, Learn).
    *   DAG task inputs/outputs.
    *   Agent-to-Agent messages.
*   Supports scoping context by:
    *   Time
    *   Logical relevance
    *   Privacy constraints (VCs)
*   Enables runtime resolution of context needed by tasks/models.
*   Tracks provenance of context used.
