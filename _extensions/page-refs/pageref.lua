-- Lua filter to add page references to cross-references in PDF output
-- Usage: Add a citation like [see @fig-example] and it will become:
--        [see Figure 1 (page XX)]

function Str(elem)
  -- Only process for PDF output
  if FORMAT ~= "pdf" then
    return elem
  end
  return elem
end

function Link(elem)
  -- Only process for PDF output
  if FORMAT ~= "pdf" then
    return elem
  end
  
  -- Check if this is a cross-reference link (starts with #)
  local target = elem.target
  if not target or not target:match("^#") then
    return elem
  end
  
  -- Extract the reference label
  local ref_label = target:sub(2) -- Remove the # prefix
  
  -- Check if the link text already contains page reference
  local link_text = pandoc.utils.stringify(elem.content)
  if link_text:match("page") then
    return elem
  end
  
  -- Add LaTeX page reference command
  -- This uses \pageref{} which works with PDF output
  local pageref_latex = " (\\pageref{" .. ref_label .. "})"
  
  -- Add the page reference to the link content
  table.insert(elem.content, pandoc.RawInline("latex", pageref_latex))
  
  return elem
end
