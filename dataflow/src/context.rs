use smallvec::SmallVec;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextKind {
    Flat,
    Repeat,
    Apply,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ScopeContext {
    id: u16,
    parent_id: Option<u16>,
    level: u8,
    kind: ContextKind,
}

impl ScopeContext {
    pub fn new(id: u16, level: u8, kind: ContextKind) -> Self {
        Self { id, parent_id: None, level, kind }
    }

    pub fn new_repeat(&self, id: u16) -> Self {
        Self { id, parent_id: Some(self.id), level: self.level + 1, kind: ContextKind::Repeat }
    }

    pub fn new_apply(&self, id: u16) -> Self {
        Self { id, parent_id: Some(self.id), level: self.level + 1, kind: ContextKind::Apply }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn level(&self) -> u8 {
        self.level
    }

    pub fn context(&self) -> ContextKind {
        self.kind
    }

    pub fn is_parent_of(&self, other: &ScopeContext) -> bool {
        if let Some(ref pid) = other.parent_id {
            self.id == *pid
        } else {
            false
        }
    }
}

pub struct ScopeContextWithOps {
    ctx: ScopeContext,
    ops_index: SmallVec<[u16; 3]>,
    parent: Option<u16>,
    children: SmallVec<[u16; 2]>,
}

impl ScopeContextWithOps {
    pub fn new(ctx: ScopeContext) -> Self {
        Self { ctx, ops_index: SmallVec::new(), parent: None, children: SmallVec::new() }
    }

    pub fn add_op(&mut self, index: u16) {
        self.ops_index.push(index);
    }

    pub fn set_parent_scope(&mut self, ctx_id: u16) {
        self.parent = Some(ctx_id)
    }

    pub fn add_child_scope(&mut self, ctx_id: u16) {
        self.children.push(ctx_id)
    }

    pub fn get_ctx(&self) -> &ScopeContext {
        &self.ctx
    }

    pub fn get_ops(&self) -> &[u16] {
        self.ops_index.as_slice()
    }

    pub fn get_children_scopes(&self) -> &[u16] {
        self.children.as_slice()
    }

    pub fn get_parent_scope(&self) -> Option<u16> {
        self.parent
    }
}
